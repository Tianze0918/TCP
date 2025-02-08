#include <arpa/inet.h>
#include <stdbool.h>
#include <stdint.h>
#include <errno.h>
#include <stddef.h>
#include <stdlib.h>
#define MSS 1012 // MSS = Maximum Segment Size (aka max length)
#define RECEIVE=1
#define SEND=0

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

void remove(PacketList* list, PacketNode* node){
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
            remove(list, found);
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

void update_input(PacketList* list, Packet* pkt, ssize_t* inputSeq, ssize_t* currentWindowSize){
    while (*inputSeq < pkt->ack){
        PacketNode* found=find(list, *inputSeq);
        if (found!=NULL){
            *inputSeq+=1;
            ssize_t packet_length=found->pkt->length;
            *currentWindowSize-=packet_length;
            remove(list, found);
        }else{
            return;
        }
    }
    
}

void get_header(Packet* pkt, uint16_t seq, uint16_t ack, uint16_t length, uint16_t flow_window, uint16_t flags){
    seq=htons(seq);
    pkt->seq=seq;
    ack=htons(ack);
    pkt->ack=ack;
    length=htons(length);
    pkt->length=length;
    flow_window=htons(flow_window);
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

void syn_ack(Packet* new_pkt, ssize_t* outputSeq, ssize_t* flowWindow){
    *outputSeq=ntohs(new_pkt->seq);
    *flowWindow=ntohs(new_pkt->window);
}

uint8_t compute_parity(Packet *pkt, size_t total_size) {
    uint8_t parity = 0;
    uint8_t *bytes = (uint8_t *)pkt;
    for (size_t i = 0; i < total_size; i++) {
        parity ^= bytes[i];
    }
    return parity;
}

bool receiver_verify_packet(Packet *pkt) {
    // Calculate total size = header size + payload size.
    size_t total_size = sizeof(Packet) + pkt->length;
    uint8_t parity = compute_parity(pkt, total_size);
    return (parity == 0);
}



// Main function of transport layer; never quits
void listen_loop(int sockfd, struct sockaddr_in* addr, int type,
                 ssize_t (*input_p)(uint8_t*, size_t),
                 void (*output_p)(uint8_t*, size_t)) {

    //Input/Output Buffer generation
    PacketList inputBuffer;
    PacketList outputBuffer;
    initPacketList(&inputBuffer);
    initPacketList(&outputBuffer);

    //Flow window initailization
    ssize_t flowWindow=1012;
    ssize_t currentWindowSize=0;

    //Sender input and out buffer index
    uint16_t inputSeq=rand()%1000;           
    uint16_t outputSeq;          //initialized in syn_ack

    //Sender's current window
    uint16_t currentWindowsStart=inputSeq;  //Initialized in syn_ack
    uint16_t currentWindowEnd=inputSeq;

    // socket address struct length used by recvfrom
    socklen_t addr_length=sizeof (*addr);


    // Input buffer to store stdin
    uint8_t buffer[MSS]={0};
    ssize_t bytes_read;

    bool client_sync=true;
    bool server_sync=true;

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
            enqueuePacket(&inputBuffer, new_pkt, 0);    //Pakcet to send are already correctly encoded
        }


        //Receiving part
        while ((bytes_read = recvfrom(sockfd, pkt, sizeof(Packet) + MSS, 0, (struct sockaddr*) addr, &addr_length)) != -1) {
            uint8_t parity=compute_parity(pkt, bytes_read);
            if (parity!=0){
                continue;   //if parity doesn't equal 0, drop the packet
            }

            //Dynamically allocate receiver packet
            Packet* new_pkt = (Packet*)malloc(bytes_read);
            if (!new_pkt) {
                perror("malloc for receiver Packet");
                exit(EXIT_FAILURE);
            }
            memcpy(new_pkt, pkt, bytes_read);

            //place arriving packets into the output buffer
            
            enqueuePacket(&outputBuffer, new_pkt, 1); //Received packet require ntohs
            ack=true;

            //Extract the flag
            uint16_t flag=new_pkt->flags; // Flag value from some packet
            bool syn = flag & 1;
            bool ack_flag = (flag >> 1) & 1;
            bool parity = (flag >> 2) & 1;


            //1. print packets to standard output
            //2. update the ack number (outputSeq)
            //3. update the flow window
            if (type==0 && syn){//Server (Respond to Client Sync)
                syn_ack(new_pkt, &outputSeq, &flowWindow);
                server_sync=true;
            }else if (type==1 && syn){//Client (Respond to Server Sync)
                syn_ack(new_pkt, &outputSeq, &flowWindow);
            }
            output(&outputBuffer, new_pkt, &outputSeq, &flowWindow, output_p);
            //clear input buffer if the ariving packet is ack
            update_input(&inputBuffer, new_pkt, inputSeq, &currentWindowSize);
        }



        //Sending part
        PacketNode* next_packet_node=find(&inputBuffer, currentWindowEnd+1);
        if (next_packet_node==NULL){ //no more packets to send
            continue;
        }else{
            Packet* next_packet=next_packet_node->pkt;
            ssize_t packet_length=ntohs(next_packet->length);
            while (currentWindowSize + packet_length < flowWindow){
                uint16_t flags;
                bool parity=compute_parity(next_packet, sizeof(Packet)+next_packet->length);
                if (type==1){   //Client
                    flags=get_flags(client_sync, ack, parity); //flags setting required
                    client_sync=false;
                }else{          //Server
                     flags=get_flags(server_sync, ack, parity); //flags setting required
                     server_sync=false;
                }
                get_header(next_packet, currentWindowEnd, outputSeq, sizeof(Packet) + next_packet->length, currentWindowSize, flags);
                sendto(sockfd, next_packet, sizeof(Packet)+next_packet->length, 0, addr, addr_length);
                currentWindowEnd+=1;
                currentWindowSize+=next_packet->length;
                next_packet=find(&inputBuffer, currentWindowEnd+1);
                if (next_packet==NULL){
                    break;
                }else{
                    packet_length=ntohs(next_packet->length);
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
                // 1. remove send buffer packets less than new ack packet seq number. (size of current window reduce)
            // b. if data packet,
                // 1. If there is available buffer inside receive buffer, store the packet inside receive buffer, else discard.
                    // If the newly arrived packet have lowest seq number in current window, write the stream formed by the current lowest
                    // numbered packet into the standard output, 
            // c. if ack+data packet
                // 1. remove send buffer packets less than new ack packet seq number. (size of current window reduce)
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
