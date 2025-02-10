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

void handle_sigint(int sig) {
    // Flush stderr to ensure any pending error messages are printed
    fflush(stderr);
    // Optionally, print a custom message
   //fprintf(stderr, "\nCaught signal %d, exiting gracefully...\n", sig);
    // Perform other cleanup tasks if necessary
    exit(0);  // Exit the program gracefully
}

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
} PacketNode;

// The linked list maintains pointers to the head and tail.
typedef struct {
    PacketNode* head;
    PacketNode* tail;
} PacketList;

// Sending queue.
typedef struct queue_node {
    Packet* pkt;
    struct queue_node* next;
    struct queue_node* previous;
    time_t send_time;
} queue_node;

// Sending queue.
typedef struct sending_queue {
    struct queue_node* head;
    struct queue_node* tail;
} sending_queue;

void initPacketList(PacketList* list) {
    list->head = NULL;
    list->tail = NULL;
}

void initSendingQueue(sending_queue* sending_queue) {
    sending_queue->head = NULL;
    sending_queue->tail = NULL;
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
           //fprintf(stderr, "Output function: pkt->seq = %hu, pkt->window= %hu, pkt->length =%hu, outputSeq = %hu, flowWindow = %hu\n", pkt->seq, pkt->window, pkt->length, *outputSeq, *receiver_flowWindow);
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

void update_input(PacketList* list,  uint16_t ack, uint16_t* currentWindowStart, uint16_t* currentWindowSize){
    ////fprintf(stderr, "Entered update_input, currentWindowStart = %hu, pkt->ack = %hu pkt->seq = %hu, pkt->flowWindow = %hu, pkt->length = %hu, pkt->flags = %hu\n", *currentWindowStart, ack, pkt->seq, pkt->window, pkt->length, pkt->flags);
      while (*currentWindowStart < ack){
         
          PacketNode* found=find(list, *currentWindowStart);
        //fprintf(stderr, "update_input\n");
          if (found!=NULL){
           //fprintf(stderr, "Clearing packet seq %hu\n", *currentWindowStart);
              *currentWindowStart+=1;
              uint16_t packet_length=found->pkt->length;
              *currentWindowSize-=packet_length;
              remove_packet(list, found);
            //fprintf(stderr, "New window size = %hu\n", *currentWindowSize);
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
        pkt->unused=ntohs(pkt->unused);
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
        pkt->unused=htons(pkt->unused);
    }
}


// Enqueue (append) a packet pointer to the list.
void enqueuePacket(PacketList* list, Packet* pkt, int type) { //type is 1 for outputbuffer, 0 for inputbuffer
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
    node->previous = NULL;
    
    if (list->tail == NULL) {  // List is empty
        list->head = node;
        list->tail = node;
    } else {
        list->tail->next = node;
        node->previous=list->tail;
        list->tail = node;
    }
    ////fprintf(stderr, "Enqueued packet size: %hu, seq = %hu, list tail seq = %hu \n", node->pkt->length, node->pkt->seq, list->tail->pkt->seq);
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
    pkt->unused=0;
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

// void retransmission(PacketList* input, uint16_t currentWindowStart, int sockfd, struct sockaddr_in* addr, socklen_t addr_length){
//    ////fprintf(stderr, "retransmission entered");
//     PacketNode* start=input->head;
//     if (start==NULL){
//         return;
//     }
//     while (start!=NULL && difftime(time(NULL), start->send_time) >= 1.0){
//         int packet_length=ntohs(start->pkt->length); //Sent Packet are encoded
//         sendto(sockfd, start->pkt, sizeof(Packet)+packet_length, 0, addr, addr_length);
//         start->send_time=time(NULL);
//         start=start->next;
//     }

// }

void send_sync(PacketList* inputBuffer, bool* client_sync, uint16_t inputSeq, uint16_t flowWindow, int sockfd, struct sockaddr_in* addr, socklen_t addr_length){
    Packet* sync_packet = (Packet*)malloc(sizeof(Packet));
    uint16_t flags=get_flags(*client_sync, 0, 0); //flags setting required
    *client_sync=false;
    // //fprintf(stderr, "Sync flag: flag = %u", flags);
    get_header(sync_packet, inputSeq, 5678, 0, 1012, flags);
    uint8_t parity=compute_parity(sync_packet, sizeof(Packet), SEND);
    if (parity==1){
        sync_packet->flags ^= ((uint16_t)1 << 2);
    }
    uint8_t result;
    enqueuePacket(inputBuffer, sync_packet, 0); 
    encode(sync_packet);
    sendto(sockfd, sync_packet, sizeof(Packet), 0, addr, addr_length);
    decode(sync_packet);
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

void send_ack(uint16_t outputSeq, int type, int sockfd, struct sockaddr_in* addr, socklen_t addr_length, uint16_t* flowWindow){
    Packet* pkt=(Packet*)malloc(sizeof(Packet));
    pkt->seq=0;
    pkt->ack=outputSeq;
    uint16_t flag=get_flags(0, true, 0);   //ack will guarantee no sync flag
    pkt->flags=flag;
    pkt->window=*flowWindow;
    pkt->length=0;
    uint8_t parity=compute_parity(pkt, sizeof(Packet), SEND);
    if (parity==1){
        pkt->flags ^= ((uint16_t)1 << 2);
    }
    encode(pkt);
    sendto(sockfd, pkt, sizeof(Packet), 0, addr, addr_length);
    free(pkt);
    //fprintf(stderr, "Ack packet with no payload:  seq = %zd, flag = %d, ack = %zd\n", pkt->seq, flag, pkt->ack);
}

// Main function of transport layer; never quits
void listen_loop(int sockfd, struct sockaddr_in* addr, int type,
                 ssize_t (*input_p)(uint8_t*, size_t),
                 void (*output_p)(uint8_t*, size_t)) {
    ////fprintf(stderr, "Inside listen_loop\n");
    //Input/Output Buffer generation
    setvbuf(stderr, NULL, _IONBF, 0); 
    PacketList inputBuffer;
    PacketList outputBuffer;
    initPacketList(&inputBuffer);
    initPacketList(&outputBuffer);

    sending_queue queue;
    initSendingQueue(&queue);

    //Flow window initailization
    uint16_t flowWindow=1012;
    uint16_t currentWindowSize=0;

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

    struct timespec now;
    static struct timespec last_print_time = {0, 0};  // will be zero initially
    clock_gettime(CLOCK_MONOTONIC, &now);

    while (true){
        bool ack=false;
        while ((bytes_read=input_p(buffer , MSS)) > 0){
            Packet* stdin_pkt = (Packet*)malloc(sizeof(Packet) + bytes_read);
            if (!stdin_pkt) {
                perror("malloc for Packet");
                exit(EXIT_FAILURE);
            }
            memcpy(stdin_pkt->payload, buffer, bytes_read);
            stdin_pkt->length = (uint16_t)bytes_read;  // Set length field, and possibly others.
            stdin_pkt->seq=inputSeq;
        //   fprintf(stderr, "Input read seq = %hu (hex): ", inputSeq);
        //     for (int i = 0; i < stdin_pkt->length; i++) {
        //         // Print each byte in two-digit hex format
        //       fprintf(stderr, "%02X ", (unsigned char)stdin_pkt->payload[i]);
        //     }
        //   fprintf(stderr, "\n");
            inputSeq+=1;
            enqueuePacket(&inputBuffer, stdin_pkt, 0);    //Pakcet to send are already correctly encoded
        }









        //Receiving part
        while ((bytes_read = recvfrom(sockfd, pkt, sizeof(Packet) + MSS, 0, (struct sockaddr*) addr, &addr_length)) != -1) {
            //Dynamically allocate receiver packet
            Packet* new_pkt = (Packet*)malloc(bytes_read);
            if (!new_pkt) {
                perror("malloc for receiver Packet");
                exit(EXIT_FAILURE);
            }
            memcpy(new_pkt, pkt, bytes_read);
            decode(new_pkt);
            uint8_t flag_parity=compute_parity(new_pkt, bytes_read, RECEIVE);
            if (flag_parity!=0){
                free(new_pkt);
                continue;   //if parity doesn't equal 0, drop the packet
            }

            enqueuePacket(&outputBuffer, new_pkt, 0); //Received packet require ntohs

            //Extract the flag
            uint16_t flag=new_pkt->flags; // Flag value from some packet
            syn = flag & 1;
            ack_flag = (flag >> 1) & 1;
            parity = (flag >> 2) & 1;
            if (syn || new_pkt->length>0){
                ack=true;
            }

            if (type==0 && syn){//Server syn-ack
                syn_ack(new_pkt, &outputSeq, &flowWindow);  
                server_sync=true;
                outputSeq+=1; //Sync message takes account of control bytes
            }else if (type==1 && syn){//Client (Respond to Server Sync)
                syn_ack(new_pkt, &outputSeq, &flowWindow);
                outputSeq+=1; //Sync message takes account of control bytes
            }
            uint16_t ack=new_pkt->ack;
            uint16_t length=new_pkt->length;
            output(&outputBuffer, new_pkt, &outputSeq, &flowWindow, output_p);
           if(ack_flag){
                update_input(&inputBuffer, ack, &currentWindowStart, &currentWindowSize);
           }
            update_flow_window(&flowWindow);
        }











        //When to send:
        //1. Packet node in input buffer have never been sent before
        //2. Timer expires for already sent node
        PacketNode* node = inputBuffer.head;
        while (node){   //if there is unsent node in the inputBuffer, send until there is no more or window size exceed
            Packet* pktToSend = node->pkt;
            uint16_t pkt_len  = pktToSend->length;
            
            // Check if there's enough "window space" left
            if (currentWindowSize + pkt_len > flowWindow) {
                // No more space; stop sending now
                break;
            }

            //set flags
            uint16_t flags;
            if (type==1 && client_sync){        //Client sync
                flags=get_flags(true, false, 0);
                client_sync=false; 
            }else if(type == 1 && syn){         //Server sync-ack
                 flags=get_flags(true, true, 0); 
            }else{                              //regular messages
                flags=get_flags(false, ack, 0);
            }
            get_header(pktToSend, pktToSend->seq, outputSeq, pktToSend->length, flowWindow, flags);


            // Compute parity, set if needed:
            uint8_t p = compute_parity(pktToSend, sizeof(Packet) + pkt_len, SEND);
            if (p == 1) {
                pktToSend->flags ^= ((uint16_t)1 << 2);
            }

            currentWindowSize += pkt_len;

            //sending packets
            encode(pktToSend);
            sendto(sockfd, pktToSend, sizeof(Packet) + pkt_len, 0, (struct sockaddr*)addr, &addr_length);
            decode(pktToSend);

            // Remove from inputBuffer (because it's now "sent")
            // but we do NOT free the packet, we keep it in an "unacked" list
            PacketNode* nextNode = node->next; // store so we don't lose it
            remove_packet(&inputBuffer, node);

            // Append it to "unackedList"
            queue_node* unackedNode = (queue_node*)malloc(sizeof(queue_node));
            unackedNode->pkt = pktToSend;
            unackedNode->next = NULL;
            unackedNode->previous = NULL;
            unackedNode->send_time=time(NULL);


            if (!queue.head) {
                queue.head = unackedNode;
                queue.tail = unackedNode;
            } else {
                unackedNode->previous = queue.tail;
                queue.tail->next = unackedNode;
                queue.tail       = unackedNode;
            }
    
            node = nextNode;  // move on to next inputBuffer item
        }

  



        

        PacketNode* next_packet_node=find(&inputBuffer, currentWindowEnd+1);
        if (next_packet_node==NULL){ //no more packets to send
            //Client sync with no payload
            if (client_sync && type==1){
               //fprintf(stderr, "Sending sync\n");  
               currentWindowEnd+=1;
                send_sync(&inputBuffer, &client_sync, inputSeq, flowWindow, sockfd, addr, addr_length);
              //fprintf(stderr, "Finished Sending sync\n"); 
            }
            //Server syn-ack with no payload
            else if(type==0 && syn){
                //fprintf(stderr, "sending syn-ack: syn=%d\n", syn);
                send_sync_ack(&server_sync, inputSeq, outputSeq, &syn, sockfd, addr, addr_length);
            }
            //Ack when no payload
            else if (ack){
                //fprintf(stderr, "sending ack: syn=%d\n", syn);
                send_ack(outputSeq, client_sync, sockfd, addr, addr_length, &flowWindow);
            }
           //fprintf(stderr, "No data\n");
            continue;
        }else{
            Packet* next_packet=next_packet_node->pkt;
            ssize_t packet_length=next_packet->length;
           //fprintf(stderr, "Sent a packet, flowwindow = %hu, currentWindowsize = %hu, packet_length = %hu seq = %hu \n", flowWindow, currentWindowSize, next_packet->length, next_packet->seq);
            while (currentWindowSize + packet_length <= flowWindow){
                uint16_t flags;
                if (type==1){   //Client
                    flags=get_flags(client_sync, ack, 0); //flags setting required
                    //fprintf(stderr, "Sending Sync packet flag, Sync: %d\n",ack, flags);
                    fflush(stderr);
                }else{          //Server
                     flags=get_flags(server_sync, ack, 0); //flags setting required
                     server_sync=false;
                }
              //fprintf(stderr, "Sending message header, Seq: %hu, Window size: %hu, packet_length: %hu\n", currentWindowEnd+1,  flowWindow, packet_length);
                get_header(next_packet, currentWindowEnd+1, outputSeq, packet_length, flowWindow, flags);
                uint8_t parity=compute_parity(next_packet, sizeof(Packet)+packet_length, SEND);
                if (parity==1){
                    next_packet->flags ^= ((uint16_t)1 << 2);
                }
               //fprintf(stderr, "Before including new packet window size = %hu\n", currentWindowSize);
                currentWindowSize+=next_packet->length;
               //fprintf(stderr, "After including new packet window size = %hu\n", currentWindowSize);
                fprintf(stderr, "Sent a packet, packet_length = %hu, seq = %hu, window size = %hu, currentWindowEnd = %hu \n", next_packet->length, next_packet->seq, currentWindowSize, currentWindowEnd);
                fprintf(stderr, "Payload (hex): ");
                for (int i = 0; i < next_packet->length; i++) {
                    // Print each byte in two-digit hex format
                    fprintf(stderr, "%02X ", (unsigned char)next_packet->payload[i]);
                }
                fprintf(stderr, "\n");
                if (client_sync){
                    enqueuePacket(&inputBuffer, next_packet, 0);
                   //fprintf(stderr, "Enqueued packet seq = %hu\n", next_packet->seq);
                    client_sync=false;
                }
                encode(next_packet);
                sendto(sockfd, next_packet, sizeof(Packet)+packet_length, 0, addr, addr_length);
                decode(next_packet);
                currentWindowEnd+=1;
               //fprintf(stderr, "current window end: %hu\n", currentWindowEnd);
                next_packet_node=find(&inputBuffer, currentWindowEnd);
               //fprintf(stderr, "\n");
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
                 

