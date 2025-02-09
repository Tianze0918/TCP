#include <arpa/inet.h>
#include <stdbool.h>
#include <stdint.h>
#include <errno.h>
#include <stddef.h>
#include <stdlib.h>
#include "consts.h"
#include <string.h>
#define MSS 1012 // MSS = Maximum Segment Size (aka max length)
#define RECEIVE 1
#define SEND 0

typedef struct Packet{
    uint16_t seq;               //input_buffer
    uint16_t ack;
    uint16_t length;            //input_buffer
    uint16_t window;
    uint16_t flags; 
    uint16_t unused;
    uint8_t payload[0];         //input_buffer
} Packet;




// Each node holds a pointer to a packet.
typedef struct PacketNode {
    Packet* pkt;
    struct PacketNode* next;
    struct PacketNode* previous;
} PacketNode;

// The linked list maintains pointers to the head and tail.
typedef struct {
    PacketNode* head;
    PacketNode* tail;
} PacketList;

void initPacketList(PacketList* list) {
    list->head = NULL;
    list->tail = NULL;
}

PacketNode* find(PacketList* list, ssize_t write_seq){
    PacketNode* start=list->head;
    while (start!=NULL){
        if (start->pkt->seq == write_seq){
            return start;
        }
        start = start->next;
    }
    return NULL;
}

void remove_packet(PacketList* list, PacketNode* node){
    if (list->head==list->tail){
        free(list->head->pkt);
        free(list->head);
        list->head=NULL;
        list->tail=NULL;
    }else if (node==list->head){
        list->head=node->next;
        list->head->previous=NULL;
        free(node->pkt);
        free(node);
    }else if(node==list->tail){
        list->tail=node->previous;
        list->tail->next=NULL;
        free(node->pkt);
        free(node);
    }else{
        node->previous->next=node->next;
        node->next->previous=node->previous;
        free(node->pkt);
        free(node);
    }
}

void output(PacketList* list, Packet* pkt, ssize_t *outputSeq, ssize_t* receiver_flowWindow, void (*output_p)(uint8_t*, size_t)){
   while (true){
        PacketNode* found=find(list, *outputSeq);
        if (found!=NULL){
            // updating next ack seq for receiver
            *outputSeq+=1;
            // Receiver side Flow window update
            uint16_t newFlowWindow=pkt->window;
            *receiver_flowWindow = (newFlowWindow) > *receiver_flowWindow  ? newFlowWindow : *receiver_flowWindow;
            output_p(found->pkt->payload, found->pkt->length);
            remove_packet(list, found);
        }else{
            return;
        }
   }
}

void decode(Packet* pkt){
    if (pkt==NULL){
        return;
    }else{
        pkt->seq=ntohs(pkt->seq);
        pkt->ack=ntohs(pkt->ack);
        pkt->length=ntohs(pkt->length);
        pkt->window=ntohs(pkt->window);
    }
}

void encode(Packet* pkt){
    if (pkt==NULL){
        return;
    }else{
        pkt->seq=htons(pkt->seq);
        pkt->ack=htons(pkt->ack);
        pkt->length=htons(pkt->length);
        pkt->window=htons(pkt->window);
    }
}


// Enqueue (append) a packet pointer to the list.
void enqueuePacket(PacketList* list, Packet* pkt, int type) {
    PacketNode* node = (PacketNode*)malloc(sizeof(PacketNode));
    if (!node) {
        perror("malloc for PacketNode");
        exit(EXIT_FAILURE);
    }
    if (type==1){   //Convert from network to byte encoding
        decode(pkt);
    }
    node->pkt = pkt;
    node->next = NULL;
    
    if (list->tail == NULL) {  // List is empty
        list->head = node;
        list->tail = node;
    } else {
        list->tail->next = node;
        node->previous=list->tail;
        list->tail = node;
    }
}

void update_input(PacketList* list, Packet* pkt, uint16_t* currentWindowStart, uint16_t* currentWindowSize){
    while (*currentWindowStart < pkt->ack){
        PacketNode* found=find(list, *currentWindowStart);
        if (found!=NULL){
            *currentWindowStart+=1;
            ssize_t packet_length=found->pkt->length;
            *currentWindowSize-=packet_length;
            remove_packet(list, found);
        }else{
            return;
        }
    }
    
}

void update_flow_window(ssize_t* flow_window){
    if (*flow_window+500<MAX_WINDOW){
        *flow_window+=500;
    }else{
        *flow_window=MAX_WINDOW;
    }
}

void get_header(Packet* pkt, uint16_t seq, uint16_t ack, uint16_t length, uint16_t flow_window, uint16_t flags){
    pkt->seq=seq;
    pkt->ack=ack;
    pkt->length=length;
    pkt->window=flow_window;
    pkt->flags=flags;
}


uint16_t get_flags(bool sync, bool ack, bool parity){
    uint16_t flag = 0;
    if (ack){
        flag|=((uint16_t)ack<<1);
    }
    if (sync){
        flag|=((uint16_t)sync);
    }
    if (parity){
        flag|=((uint16_t)parity<<2);
    }
    return flag;
}

void syn_ack(Packet* new_pkt, uint16_t* outputSeq, ssize_t* flowWindow){
    *outputSeq=(new_pkt->seq);
    *flowWindow=(new_pkt->window);
}

uint8_t compute_parity(Packet *pkt, size_t total_size, int type) {
    uint8_t parity = 0;
    uint8_t *bytes = (uint8_t *)pkt;
    for (size_t i = 0; i < total_size; i++) {
        parity ^= bytes[i];
    }
    if (type==SEND && PARITY==1){
        parity=0;
    }
    return parity;
}

bool receiver_verify_packet(Packet *pkt) {
    // Calculate total size = header size + payload size.
    size_t total_size = sizeof(Packet) + pkt->length;
    uint8_t parity = compute_parity(pkt, total_size, RECEIVE);
    return (parity == 0);
}



// Main function of transport layer; never quits
void listen_loop(int sockfd, struct sockaddr_in* addr, int type,
                 ssize_t (*input_p)(uint8_t*, size_t),
                 void (*output_p)(uint8_t*, size_t)) {
    //fprintf(stderr, "Inside listen_loop\n");
    //Input/Output Buffer generation
    PacketList inputBuffer;
    PacketList outputBuffer;
    initPacketList(&inputBuffer);
    initPacketList(&outputBuffer);

    //Flow window initailization
    ssize_t flowWindow=1012;
    ssize_t currentWindowSize=0;

    //Sender input and out buffer index
    srand(time(NULL));
    uint16_t inputSeq=rand()%1000;   
    //fprintf(stderr, "inputSeq initialized to %hu\n", inputSeq);     
    uint16_t outputSeq;          //initialized in syn_ack

    //Sender's current window
    uint16_t currentWindowStart=inputSeq;  //Initialized in syn_ack
    uint16_t currentWindowEnd=inputSeq-1;

    // socket address struct length used by recvfrom
    socklen_t addr_length=sizeof (*addr);


    // Input buffer to store stdin
    uint8_t buffer[MSS]={0};
    ssize_t bytes_read;

    bool client_sync=true;
    bool server_sync=true;

    //Received packet flag
    bool syn;
    bool ack_flag;
    bool parity;

    //Receiver buffer
    char recv_buf[sizeof(Packet) + MSS] = {0};
    Packet* pkt = (Packet*) recv_buf;

    while (true){
        //determine if the ack flag should be set
        bool ack=false;

        // Place stdin into input buffer
        while ((bytes_read=input_p(buffer , MSS)) > 0){
            Packet* new_pkt = (Packet*)malloc(sizeof(Packet) + bytes_read);
            if (!new_pkt) {
                perror("malloc for Packet");
                exit(EXIT_FAILURE);
            }
            memcpy(new_pkt->payload, buffer, bytes_read);
            new_pkt->length = (uint16_t)bytes_read;  // Set length field, and possibly others.
            new_pkt->seq=inputSeq;
            inputSeq+=1;
            //fprintf(stderr, "inputSeq: %hu\n", inputSeq);  
            enqueuePacket(&inputBuffer, new_pkt, 0);    //Pakcet to send are already correctly encoded
        }


        //Receiving part
        while ((bytes_read = recvfrom(sockfd, pkt, sizeof(Packet) + MSS, 0, (struct sockaddr*) addr, &addr_length)) != -1) {
            //fprintf(stderr, "Inside receive loop\n");
            uint8_t flag_parity=compute_parity(pkt, bytes_read, RECEIVE);
            // if (flag_parity!=0){
            //     continue;   //if parity doesn't equal 0, drop the packet
            // }

            //Dynamically allocate receiver packet
            Packet* new_pkt = (Packet*)malloc(bytes_read);
            if (!new_pkt) {
                perror("malloc for receiver Packet");
                exit(EXIT_FAILURE);
            }
            memcpy(new_pkt, pkt, bytes_read);

            //place arriving packets into the output buffer
            
            //fprintf(stderr, "Enqueuepacket, inputSeq: %hu\n", inputSeq); 
            enqueuePacket(&outputBuffer, new_pkt, 1); //Received packet require ntohs
            ack=true;

            //Extract the flag
            uint16_t flag=new_pkt->flags; // Flag value from some packet
            syn = flag & 1;
            ack_flag = (flag >> 1) & 1;
            parity = (flag >> 2) & 1;


            //1. print packets to standard output
            //2. update the ack number (outputSeq)
            //3. update the flow window
            if (type==0 && syn){//Server syn-ack
                //fprintf(stderr, "syn-ack start, inputSeq: %hu\n", inputSeq); 
                syn_ack(new_pkt, &outputSeq, &flowWindow);  
                //fprintf(stderr, "Inside server response to sync: outputSeq = %zd, flowWindow = %zd\n", outputSeq, flowWindow);
                server_sync=true;
                outputSeq+=1; //Sync message takes account of control bytes
                //fprintf(stderr, "syn-ack end, inputSeq: %hu\n", inputSeq); 
            }else if (type==1 && syn){//Client (Respond to Server Sync)
                //fprintf(stderr, "Inside client response to sync before:  outputSeq = %zd, flowWindow = %zd\n", outputSeq, flowWindow);
                syn_ack(new_pkt, &outputSeq, &flowWindow);
                outputSeq+=1; //Sync message takes account of control bytes
                //fprintf(stderr, "Inside client response to sync after:  outputSeq = %zd, flowWindow = %zd\n", outputSeq, flowWindow);
            }
            output(&outputBuffer, new_pkt, &outputSeq, &flowWindow, output_p);
            //clear input buffer if the ariving packet is ack
            //fprintf(stderr, "update_input, inputSeq: %hu\n", inputSeq); 
            update_input(&inputBuffer, new_pkt, &currentWindowStart, &currentWindowSize);
            //Increment the flow window each time we successfully receive a packet from the other side
            //fprintf(stderr, "update_flow_window, inputSeq: %hu\n", inputSeq); 
            update_flow_window(&flowWindow);
        }

        //Sending part
        PacketNode* next_packet_node=find(&inputBuffer, currentWindowEnd+1);
        if (next_packet_node==NULL){ //no more packets to send
            //Client sync with no payload
            if (client_sync && type==1){
                Packet* sync_packet = (Packet*)malloc(sizeof(Packet));
                bool parity=compute_parity(sync_packet, sizeof(Packet), SEND);
                uint16_t flags=get_flags(client_sync, 0, parity); //flags setting required
                client_sync=false;
                get_header(sync_packet, 1234, 5678, sizeof(*sync_packet), 1012, flags);
                encode(sync_packet);
                sendto(sockfd, sync_packet, sizeof(Packet), 0, addr, addr_length);
                currentWindowEnd+=1;
                enqueuePacket(&inputBuffer, sync_packet, 0); 
                //fprintf(stderr, "Client Sync packet(No data):  inputSeq = %zd, flowWindow = %zd\n", inputSeq, flowWindow);
            }
            //Server syn-ack with no payload
            else if(type==0 && syn){
                Packet* pkt=(Packet*)malloc(sizeof(Packet));
                pkt->seq=inputSeq;
                pkt->ack=outputSeq;
                pkt->window=1012;
                uint16_t flag=get_flags(server_sync, true, false);
                pkt->flags=flag;
                encode(pkt);
                sendto(sockfd, pkt, sizeof(Packet), 0, addr, addr_length);
                free(pkt);
                syn=0;
                //fprintf(stderr, "Syn-ack packet:  seq = %hu, flag = %hu, ack = %hu\n", inputSeq, flag, outputSeq);
            }
            //Ack when no payload
            else if (ack){
                Packet* pkt=(Packet*)malloc(sizeof(Packet));
                pkt->seq=0;
                pkt->ack=outputSeq;
                uint16_t flag=get_flags(client_sync,true,false);
                pkt->flags=flag;
                encode(pkt);
                sendto(sockfd, pkt, sizeof(Packet), 0, addr, addr_length);
                free(pkt);
                //fprintf(stderr, "Ack packet with no payload:  seq = %zd, flag = %d, ack = %zd\n", pkt->seq, flag, pkt->ack);
            }
            continue;
        }else{
            Packet* next_packet=next_packet_node->pkt;
            encode(next_packet);
            ssize_t packet_length=next_packet->length;
            while (currentWindowSize + packet_length < flowWindow){
                uint16_t flags;
                bool parity=compute_parity(next_packet, sizeof(Packet)+packet_length, SEND);
                if (type==1){   //Client
                    flags=get_flags(client_sync, ack, parity); //flags setting required
                    client_sync=false;
                }else{          //Server
                     flags=get_flags(server_sync, ack, parity); //flags setting required
                     server_sync=false;
                }
                get_header(next_packet, currentWindowEnd, outputSeq, packet_length, flowWindow, flags);
                sendto(sockfd, next_packet, sizeof(Packet)+packet_length, 0, addr, addr_length);
                currentWindowEnd+=1;
                currentWindowSize+=next_packet->length;
                next_packet=find(&inputBuffer, currentWindowEnd+1);
                if (next_packet==NULL){
                    break;
                }else{
                    packet_length=next_packet->length;
                }
            }
        }




    }

                








    // We should have a starting sequence number for server at this step
    // uint16_t seq; 
    // while (true) {
        // steps: 
        // 1 read in the received packet (3 scenarios: 1. Ack packet 2. Data packet 3. Ack+Data packet)
            // a. if ack packet, 
                // 1. remove_packet send buffer packets less than new ack packet seq number. (size of current window reduce)
            // b. if data packet,
                // 1. If there is available buffer inside receive buffer, store the packet inside receive buffer, else discard.
                    // If the newly arrived packet have lowest seq number in current window, write the stream formed by the current lowest
                    // numbered packet into the standard output, 
            // c. if ack+data packet
                // 1. remove_packet send buffer packets less than new ack packet seq number. (size of current window reduce)
                // 2.If there is available buffer inside receive buffer, store the packet inside receive buffer, else discard.
                    // If the newly arrived packet have lowest seq number in current window, write the stream formed by the current lowest
                    // numbered packet into the standard output, 
        // 2 read all standard input 
            // a. parse the standard input into multiple packet
            // b. save the input packet into send buffer
            // c. Compute the number of packets that I can send (available in receiver's receive buffer) (Flow window size from other side - size of current window)
            // d. send the packets I can send to receiver. 
    // }
}
