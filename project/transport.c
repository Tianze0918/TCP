#include <arpa/inet.h>
#include <stdbool.h>
#include <stdint.h>
#include <errno.h>
#include <stddef.h>
#include <stdlib.h>
#include "consts.h"
#include <string.h>
#include <time.h>
#define MSS 1012 // MSS = Maximum Segment Size (aka max length)
#define RECEIVE 1
#define SEND 0
#define QUEUE_SIZE 1012

typedef struct Packet{
    uint16_t seq;               //input_buffer
    uint16_t ack;
    uint16_t length;            //input_buffer
    uint16_t window;
    uint16_t flags; 
    uint16_t unused;
    uint8_t payload[0];         //input_buffer
} Packet;


// typedef struct {
//     packet packets[QUEUE_SIZE];
//     int front;                   
//     int rear;                    
//     int count;                   
// } pQueue;

// void init_queue(pQueue *q) {
//     q->front = 0;
//     q->rear = 0;
//     q->count = 0;
// }

// int enqueue(pQueue *q, packet *pkt) {
//     if (q->count == QUEUE_SIZE) {
//         printf("queue is already full\n");
//         return -1; 
//     }

//     q->packets[q->rear] = *pkt; 
//     q->rear = (q->rear + 1) % QUEUE_SIZE;
//     q->count++;
//     return 0;
// }

// int check_queue(pQueue *q, uint16_t seq) {
//     for (int i; i < q->count; i++) {
//         packet *pkt = &q->packets[i];
//         if (pkt->seq < seq) {
//             dequeue(q, pkt);
//         }
//     }
// }

// int dequeue(pQueue *q, packet *pkt) {
//     if (q->count == 0) {
//         printf("queue is empty\n");
//         return -1; 
//     }

//     *pkt = q->packets[q->front];
//     q->front = (q->front + 1) % QUEUE_SIZE;
//     q->count--;
//     return 0;
// }

// int send_packet(int sockfd, struct sockaddr_in *dest_addr, pQueue *q) {
//     if (q->count == 0) {
//         printf("no packets in queue\n");
//         return -1;
//     }

//     packet pkt;
//     if (dequeue(q, &pkt) == 0) {
//         int sent_bytes = sendto(sockfd, &pkt, sizeof(packet), 0,
//                                 (struct sockaddr *) dest_addr, sizeof(*dest_addr));
//         if (sent_bytes < 0) {
//             perror("sendto failed");
//             return -1;
//         }
//         printf("sent packet with seq: %d\n", ntohs(pkt.seq));
//     }

//     return 0;
// }



// Each node holds a pointer to a packet.
typedef struct PacketNode {
    Packet* pkt;
    struct PacketNode* next;
    struct PacketNode* previous;
    time_t send_time;
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

PacketNode* find(PacketList* list, uint16_t write_seq){
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

void output(PacketList* list, Packet* pkt, uint16_t *outputSeq, uint16_t* receiver_flowWindow, void (*output_p)(uint8_t*, size_t)){
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
    // fprintf(stderr, "Enqueued packet size: %u\n", node->pkt->length);
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

void update_flow_window(uint16_t* flow_window){
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

void syn_ack(Packet* new_pkt, uint16_t* outputSeq, uint16_t* flowWindow){
    *outputSeq=(new_pkt->seq);
    *flowWindow=(new_pkt->window);
}

uint8_t compute_parity(Packet *pkt, size_t total_size, int type) {
    uint8_t parity = 0;
    // Cast the packet pointer to a pointer to bytes.
    uint8_t *bytes = (uint8_t *)pkt;
    
    // Loop through each byte of the packet.
    for (size_t i = 0; i < total_size; i++) {
        // Loop through each bit in the current byte.
        for (int bit = 0; bit < 8; bit++) {
            // Extract the individual bit (0 or 1) and XOR it with the running parity.
            parity ^= ((bytes[i] >> bit) & 1);
        }
    }
    
    // The function now returns a single-bit value (0 or 1)
    return parity;
}

bool receiver_verify_packet(Packet *pkt) {
    // Calculate total size = header size + payload size.
    size_t total_size = sizeof(Packet) + pkt->length;
    uint8_t parity = compute_parity(pkt, total_size, RECEIVE);
    return (parity == 0);
}

void retransmission(PacketList* input, uint16_t currentWindowStart, int sockfd, struct sockaddr_in* addr, socklen_t addr_length){
   ////fprintf(stderr, "retransmission entered");
    PacketNode* start=input->head;
    if (start==NULL){
        return;
    }
    while (start!=NULL && difftime(time(NULL), start->send_time) >= 1.0){
        int packet_length=ntohs(start->pkt->length); //Sent Packet are encoded
        sendto(sockfd, start->pkt, sizeof(Packet)+packet_length, 0, addr, addr_length);
        start->send_time=time(NULL);
        start=start->next;
    }

}

void send_sync(PacketList* inputBuffer, bool* client_sync, uint16_t inputSeq, uint16_t* currentWindowEnd, uint16_t flowWindow, int sockfd, struct sockaddr_in* addr, socklen_t addr_length){
    Packet* sync_packet = (Packet*)malloc(sizeof(Packet));
    uint16_t flags=get_flags(*client_sync, 0, 0); //flags setting required
    *client_sync=false;
    // //fprintf(stderr, "Sync flag: flag = %u", flags);
    get_header(sync_packet, inputSeq, 5678, sizeof(*sync_packet), 1012, flags);
    uint8_t parity=compute_parity(sync_packet, sizeof(Packet), SEND);
    if (parity==1){
        sync_packet->flags ^= ((uint16_t)1 << 2);
    }
    uint8_t result;
    encode(sync_packet);
    sendto(sockfd, sync_packet, sizeof(Packet), 0, addr, addr_length);
    *currentWindowEnd+=1;
    enqueuePacket(inputBuffer, sync_packet, 0); 
//    //fprintf(stderr, "Client Sync packet(No data):  inputSeq = %zd, flowWindow = %zd\n", inputSeq, flowWindow);
}

void send_sync_ack(bool* server_sync, uint16_t inputSeq, uint16_t outputSeq, bool* syn, int sockfd, struct sockaddr_in* addr, socklen_t addr_length){
    Packet* pkt=(Packet*)malloc(sizeof(Packet));
    pkt->seq=inputSeq;
    pkt->ack=outputSeq;
    pkt->window=1012;
    uint16_t flag=get_flags(*server_sync, true, 0);
    pkt->flags=flag;
    uint8_t parity=compute_parity(pkt, sizeof(Packet), SEND);
    if (parity==1){
        pkt->flags ^= ((uint16_t)1 << 2);
    }
    encode(pkt);
    sendto(sockfd, pkt, sizeof(Packet), 0, addr, addr_length);
    free(pkt);
    *syn=false;
    ////fprintf(stderr, "Syn-ack packet:  seq = %hu, flag = %hu, ack = %hu\n", inputSeq, flag, outputSeq);
}

void send_ack(uint16_t outputSeq, int type, int sockfd, struct sockaddr_in* addr, socklen_t addr_length){
    Packet* pkt=(Packet*)malloc(sizeof(Packet));
    pkt->seq=0;
    pkt->ack=outputSeq;
    uint16_t flag=get_flags(0, true, 0);   //ack will guarantee no sync flag
    pkt->flags=flag;
    uint8_t parity=compute_parity(pkt, sizeof(Packet), SEND);
    if (parity==1){
        pkt->flags ^= ((uint16_t)1 << 2);
    }
    encode(pkt);
    sendto(sockfd, pkt, sizeof(Packet), 0, addr, addr_length);
    free(pkt);
    ////fprintf(stderr, "Ack packet with no payload:  seq = %zd, flag = %d, ack = %zd\n", pkt->seq, flag, pkt->ack);
}

// Main function of transport layer; never quits
void listen_loop(int sockfd, struct sockaddr_in* addr, int type,
                 ssize_t (*input_p)(uint8_t*, size_t),
                 void (*output_p)(uint8_t*, size_t)) {
    // fprintf(stderr, "Inside listen_loop\n");
    //Input/Output Buffer generation
    PacketList inputBuffer;
    PacketList outputBuffer;
    initPacketList(&inputBuffer);
    initPacketList(&outputBuffer);

    //Flow window initailization
    uint16_t flowWindow=1012;
    uint16_t currentWindowSize=0;

    //Sender input and out buffer index
    srand(time(NULL));
    uint16_t inputSeq=rand()%1000;   
    ////fprintf(stderr, "inputSeq initialized to %hu\n", inputSeq);     
    uint16_t outputSeq;          //initialized in syn_ack

    //Sender's current window
    uint16_t currentWindowStart=inputSeq;  //Initialized in syn_ack
    uint16_t currentWindowEnd=inputSeq+1;

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

    struct timespec now;
    static struct timespec last_print_time = {0, 0};  // will be zero initially
    clock_gettime(CLOCK_MONOTONIC, &now);

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
            ////fprintf(stderr, "inputSeq: %hu\n", inputSeq);  
            enqueuePacket(&inputBuffer, new_pkt, 0);    //Pakcet to send are already correctly encoded
            fprintf(stderr, "Taking in input:  packet_length = %hu, inputSeq = %hu\n", new_pkt->length, inputSeq);
        }
        // fprintf(stderr, "bytes_read = %hu", bytes_read);


        //Receiving part
        while ((bytes_read = recvfrom(sockfd, pkt, sizeof(Packet) + MSS, 0, (struct sockaddr*) addr, &addr_length)) != -1) {
            fprintf(stderr, "Inside receive loop\n");
            uint8_t flag_parity=compute_parity(pkt, bytes_read, RECEIVE);
            if (flag_parity!=0){
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
            
            ////fprintf(stderr, "Enqueuepacket, inputSeq: %hu\n", inputSeq); 
            enqueuePacket(&outputBuffer, new_pkt, 1); //Received packet require ntohs

            //Extract the flag
            uint16_t flag=new_pkt->flags; // Flag value from some packet
            syn = flag & 1;
            ack_flag = (flag >> 1) & 1;
            parity = (flag >> 2) & 1;
            if (syn || new_pkt->length>0){
                ack=true;
            }
            //fprintf(stderr, "Ack val: %d\n", ack); 

            //1. print packets to standard output
            //2. update the ack number (outputSeq)
            //3. update the flow window
            if (type==0 && syn){//Server syn-ack
                ////fprintf(stderr, "syn-ack start, inputSeq: %hu\n", inputSeq); 
                syn_ack(new_pkt, &outputSeq, &flowWindow);  
                ////fprintf(stderr, "Inside server response to sync: outputSeq = %zd, flowWindow = %zd\n", outputSeq, flowWindow);
                server_sync=true;
                outputSeq+=1; //Sync message takes account of control bytes
                ////fprintf(stderr, "syn-ack end, inputSeq: %hu\n", inputSeq); 
            }else if (type==1 && syn){//Client (Respond to Server Sync)
                ////fprintf(stderr, "Inside client response to sync before:  outputSeq = %zd, flowWindow = %zd\n", outputSeq, flowWindow);
                syn_ack(new_pkt, &outputSeq, &flowWindow);
                outputSeq+=1; //Sync message takes account of control bytes
                ////fprintf(stderr, "Inside client response to sync after:  outputSeq = %zd, flowWindow = %zd\n", outputSeq, flowWindow);
            }
            output(&outputBuffer, new_pkt, &outputSeq, &flowWindow, output_p);
            //clear input buffer if the ariving packet is ack
            ////fprintf(stderr, "update_input, inputSeq: %hu\n", inputSeq); 
            update_input(&inputBuffer, new_pkt, &currentWindowStart, &currentWindowSize);
            //Increment the flow window each time we successfully receive a packet from the other side
            ////fprintf(stderr, "update_flow_window, inputSeq: %hu\n", inputSeq); 
            update_flow_window(&flowWindow);
        }
        // fprintf(stderr, "bytes_read = %hu", bytes_read);

        //Sending part (new packets)
       ////fprintf(stderr, "Sending part\n"); 
        PacketNode* next_packet_node=find(&inputBuffer, currentWindowEnd+1);
        if (next_packet_node==NULL){ //no more packets to send
            //Client sync with no payload
            if (client_sync && type==1){
               //fprintf(stderr, "Sending sync\n");  
                send_sync(&inputBuffer, &client_sync, inputSeq, &currentWindowEnd, flowWindow, sockfd, addr, addr_length);
               ////fprintf(stderr, "Finished Sending sync\n"); 
            }
            //Server syn-ack with no payload
            else if(type==0 && syn){
                //fprintf(stderr, "sending syn-ack: syn=%d\n", syn);
                send_sync_ack(&server_sync, inputSeq, outputSeq, &syn, sockfd, addr, addr_length);
            }
            //Ack when no payload
            else if (ack){
                //fprintf(stderr, "sending ack: syn=%d\n", syn);
                send_ack(outputSeq, client_sync, sockfd, addr, addr_length);
            }
            fprintf(stderr, "No data\n");
            continue;
        }else{
            Packet* next_packet=next_packet_node->pkt;
            ssize_t packet_length=next_packet->length;

            long elapsed_ns = (now.tv_sec - last_print_time.tv_sec) * 1000000000L + (now.tv_nsec - last_print_time.tv_nsec);
            if (elapsed_ns >= 200000000 || last_print_time.tv_sec == 0) {
                fprintf(stderr, "There is data, CurrentWindowEnd = %hu, currentWindowSize = %hu, packet_length = %hu, flowWindow = %hu\n", currentWindowEnd, currentWindowSize, packet_length, flowWindow);
                last_print_time = now;
            }

            while (currentWindowSize + packet_length <= flowWindow){
                fprintf(stderr, "Entered sending loop, CurrentWindowEnd = %hu\n", currentWindowEnd);
                uint16_t flags;
                if (type==1){   //Client
                    flags=get_flags(client_sync, ack, 0); //flags setting required
                    client_sync=false;
                }else{          //Server
                     flags=get_flags(server_sync, ack, 0); //flags setting required
                     server_sync=false;
                }
                get_header(next_packet, currentWindowEnd, outputSeq, packet_length, flowWindow, flags);
                uint8_t parity=compute_parity(next_packet, sizeof(Packet)+packet_length, SEND);
                if (parity==1){
                    next_packet->flags ^= ((uint16_t)1 << 2);
                }
                // fprintf(stderr, "Sending packets size: %hu\n", next_packet->length);
                encode(next_packet);
                sendto(sockfd, next_packet, sizeof(Packet)+packet_length, 0, addr, addr_length);
                next_packet_node->send_time=time(NULL);
                currentWindowEnd+=1;
                currentWindowSize+=next_packet->length;
                next_packet_node=find(&inputBuffer, currentWindowEnd+1);
                if (next_packet_node==NULL){
                    break;
                }else{
                    next_packet=next_packet_node->pkt;
                    packet_length=next_packet->length;
                }
            }
        }

        //retransmission part
        // retransmission(&inputBuffer, currentWindowStart, sockfd, addr, addr_length);
    }
}
                 

