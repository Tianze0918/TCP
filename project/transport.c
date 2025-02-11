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

queue_node* find_queue_node(sending_queue* list, uint16_t seq){
    queue_node* start=list->head;
    while (start!=NULL){
        if (start->pkt->seq == seq){
            return start;
        }
        start = start->next;
    }
    return NULL;
}

PacketNode* find_packet_node(PacketList* list, uint16_t seq){
    PacketNode* start=list->head;
    while (start!=NULL){
        if (start->pkt->seq == seq){
            return start;
        }
        start = start->next;
    }
    return NULL;
}

void remove_queue_packet(sending_queue* list, queue_node* node){
    // fprintf(stderr, "Clearing node seq = %hu\n", node->pkt->length);
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

void remove_buffer_packet(PacketList* list, PacketNode* node){
    if (list->head==list->tail){
        free(list->head);
        list->head=NULL;
        list->tail=NULL;
    }else if (node==list->head){
        list->head=node->next;
        list->head->previous=NULL;
        free(node);
    }else if(node==list->tail){
        list->tail=node->previous;
        list->tail->next=NULL;
        free(node);
    }else{
        node->previous->next=node->next;
        node->next->previous=node->previous;
        free(node);
    }
}


void output(PacketList* list, Packet* pkt, uint16_t *outputSeq, uint16_t* receiver_flowWindow, void (*output_p)(uint8_t*, size_t)){
    fprintf(stderr, "%hu\n", *outputSeq);
   while (true){
        PacketNode* found=find_packet_node(list, *outputSeq);
        if (found!=NULL){
           //fprintf(stderr, "Output function: pkt->seq = %hu, pkt->window= %hu, pkt->length =%hu, outputSeq = %hu, flowWindow = %hu\n", pkt->seq, pkt->window, pkt->length, *outputSeq, *receiver_flowWindow);
            // updating next ack seq for receiver
            *outputSeq+=1;
            // Receiver side Flow window update
            uint16_t newFlowWindow=pkt->window;
            *receiver_flowWindow = (newFlowWindow) > *receiver_flowWindow  ? newFlowWindow : *receiver_flowWindow;
            output_p(found->pkt->payload, found->pkt->length);
            remove_buffer_packet(list, found);
        }else{
            return;
        }
   }
}

void update_sending_queue(sending_queue* sending_queue,  uint16_t ack, uint16_t* currentWindowSize){
    //fprintf(stderr, "Updating sending queue node seq = %hu, ack = %hu\n", sending_queue->head->pkt->seq, ack);
      while ((sending_queue->head) && sending_queue->head->pkt->seq < ack){
        //fprintf(stderr, "Removing sending queue node seq = %hu\n", sending_queue->head->pkt->seq);
        queue_node* headNode = sending_queue->head;
        *currentWindowSize -= headNode->pkt->length;
        remove_queue_packet(sending_queue, headNode);
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

bool decode_flags(uint16_t flags){
    bool syn = ((flags & 0x0001) != 0);           // bit 0

    // Return true if either SYN or ACK is set
    if (syn) {
        return true;
    }
    return false;
}

void syn_ack(Packet* new_pkt, uint16_t* outputSeq, uint16_t* flowWindow, uint16_t* last_ack){
    *outputSeq=(new_pkt->seq);
    *flowWindow=(new_pkt->window);
    *last_ack=new_pkt->ack;
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

void retransmission(sending_queue* input, int sockfd, struct sockaddr_in* addr, socklen_t addr_length){
    queue_node* start=input->head;
    if (start==NULL){
        return;
    }
    if (start!=NULL){
        // if (difftime(time(NULL), start->send_time) >= 1.0){
        //     while (start!=NULL){
        //         fprintf(stderr, "Sendingqueue seq = %hu\n", start->pkt->seq);
        //         start=start->next;
        //     }
        //     fprintf(stderr, "\n");
        // }
        if (difftime(time(NULL), start->send_time) >= 1.0){
            fprintf(stderr, "retransmission triggered, seq = %hu\n", start->pkt->seq);
            int packet_length=start->pkt->length; //Sent Packet are encoded
            encode(start->pkt);
            sendto(sockfd, start->pkt, sizeof(Packet)+packet_length, 0, addr, addr_length);
            decode(start->pkt);
            start->send_time=time(NULL);
        }
    }
}

void no_payload_sync(uint16_t* inputSeq, sending_queue* queue, PacketList* inputBuffer){
    Packet* stdin_pkt = (Packet*)malloc(sizeof(Packet));
    if (!stdin_pkt) {
        perror("malloc for Packet");
        exit(EXIT_FAILURE);
    }
    stdin_pkt->length = 0;  // Set length field, and possibly others.
    stdin_pkt->seq=*inputSeq;
    uint16_t flags=get_flags(true, false, 0);
    stdin_pkt->flags=flags;
    *inputSeq+=1;
    enqueuePacket(inputBuffer, stdin_pkt, 0);
}

void send_sync_ack(uint16_t inputSeq, uint16_t outputSeq, int sockfd, struct sockaddr_in* addr, socklen_t addr_length){
    Packet* pkt=(Packet*)malloc(sizeof(Packet));
    pkt->seq=inputSeq;
    pkt->ack=outputSeq;
    pkt->window=1012;
    uint16_t flag=get_flags(true, true, 0);
    pkt->flags=flag;
    uint8_t parity=compute_parity(pkt, sizeof(Packet), SEND);
    if (parity==1){
        pkt->flags ^= ((uint16_t)1 << 2);
    }
    encode(pkt);
    sendto(sockfd, pkt, sizeof(Packet), 0, addr, addr_length);
    free(pkt);
   (stderr, "Syn-ack packet:  seq = %hu, flag = %hu, ack = %hu\n", inputSeq, flag, outputSeq);
}

void send_ack(uint16_t outputSeq, int sockfd, struct sockaddr_in* addr, socklen_t addr_length, uint16_t* flowWindow){
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


    uint16_t last_ack;
    uint16_t counter=0;

    while (true){
        bool ack=false;
        if ((bytes_read=input_p(buffer , MSS)) > 0){
        // while ((bytes_read=input_p(buffer , MSS)) > 0){
            Packet* stdin_pkt = (Packet*)malloc(sizeof(Packet) + bytes_read);
            if (!stdin_pkt) {
                perror("malloc for Packet");
                exit(EXIT_FAILURE);
            }
            memcpy(stdin_pkt->payload, buffer, bytes_read);
            stdin_pkt->length = (uint16_t)bytes_read;  // Set length field, and possibly others.
            stdin_pkt->seq=inputSeq;
            //initializing with sync packet
            if (client_sync){
                uint16_t flags=get_flags(true, false, 0);
                stdin_pkt->flags=flags;
                client_sync=false;
               //frpintf(stderr, "Sync Packet have payload\n");
            }
            ////frpintf(stderr, "Test started\n");
         //frpintf(stderr, "Taking input, seq = %hu (hex): ", inputSeq);
            for (int i = 0; i < stdin_pkt->length; i++) {
                // Print each byte in two-digit hex format
             //frpintf(stderr, "%02X ", (unsigned char)stdin_pkt->payload[i]);
            }
         //frpintf(stderr, "\n");
            inputSeq+=1;
            enqueuePacket(&inputBuffer, stdin_pkt, 0);    //Pakcet to send are already correctly encoded
        }








        if ((bytes_read = recvfrom(sockfd, pkt, sizeof(Packet) + MSS, 0, (struct sockaddr*) addr, &addr_length)) != -1) {
        //Receiving part
        // while ((bytes_read = recvfrom(sockfd, pkt, sizeof(Packet) + MSS, 0, (struct sockaddr*) addr, &addr_length)) != -1) {
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

            uint16_t ack=new_pkt->ack;
            fprintf(stderr, "Inside receive, recvied packet seq = %hu, length = %hu, ack = %hu \n", new_pkt->seq, new_pkt->length, ack);
            uint16_t length=new_pkt->length;
            if (ack==last_ack){
                counter+=1;
            }else{
                counter=0;
                last_ack=ack;
            }
            if (type==0 && syn){//Server syn-ack
               //frpintf(stderr, "inside server's sync ack");
                syn_ack(new_pkt, &outputSeq, &flowWindow, &last_ack);  
                outputSeq+=1; //Sync message takes account of control bytes
                send_sync_ack(inputSeq, outputSeq, sockfd, addr, addr_length);
                counter=1;
            }else if (type==1 && syn){//Client (Respond to Server Sync)
                syn_ack(new_pkt, &outputSeq, &flowWindow, &last_ack);
                output(&outputBuffer, new_pkt, &outputSeq, &flowWindow, output_p);
                // outputSeq+=1; //Sync message takes account of control bytes
                send_ack(outputSeq+1, sockfd, addr, addr_length, &flowWindow);
                counter=1;
            }else if (new_pkt->seq >= outputSeq){         //send an ack if arriving packet's sequence number > outputseq
                output(&outputBuffer, new_pkt, &outputSeq, &flowWindow, output_p);
                send_ack(outputSeq, sockfd, addr, addr_length, &flowWindow);
            }else if (new_pkt->seq < outputSeq){
                send_ack(outputSeq, sockfd, addr, addr_length, &flowWindow);
            }
           if(ack_flag){
                update_sending_queue(&queue, ack, &currentWindowSize);
           }
            update_flow_window(&flowWindow);
        }









        //sync packet trigger with no payload
        if (client_sync){
           //frpintf(stderr, "No payload");
            no_payload_sync(&inputSeq, &queue, &inputBuffer);
            client_sync=false;
        }

        retransmission(&queue, sockfd, addr, addr_length);

        //sending three duplicate
        if (counter==3){
           //frpintf(stderr, "Three duplicate: seq = %hu\n", last_ack);
            if (queue.head==NULL){
               //frpintf(stderr, "three duplicate on empty queue");
                counter=0;
            }
            else{
                Packet* pkt = queue.head->pkt;
                uint16_t pkt_length=pkt->length;
                encode(pkt);
                sendto(sockfd, pkt, sizeof(Packet) + pkt_length, 0, addr, addr_length);
                decode(pkt);
                counter=0;
            }
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
            uint16_t flags = pktToSend->flags;
            //if current packet is the sync packet, don't reset flags
            if (!decode_flags(flags)){
                ////frpintf(stderr, "decode flags\n");
                flags=get_flags(false, false, 0);
            }
            get_header(pktToSend, pktToSend->seq, outputSeq, pktToSend->length, flowWindow, flags);


            // Compute parity, set if needed:
            uint8_t p = compute_parity(pktToSend, sizeof(Packet) + pkt_len, SEND);
            if (p == 1) {
                pktToSend->flags ^= ((uint16_t)1 << 2);
            }

            currentWindowSize += pkt_len;

            //sending packets
           //fprintf(stderr, "sent packet flag = %hu, packet length = %hu, packet seq=%hu\n", pktToSend->flags, pktToSend->length, pktToSend->seq);
           //frpintf(stderr, "Input read seq = %hu (hex): ", inputSeq);
            for (int i = 0; i < pktToSend->length; i++) {
                // Print each byte in two-digit hex format
             //frpintf(stderr, "%02X ", (unsigned char)pktToSend->payload[i]);
            }
           //frpintf(stderr, "\n");
            encode(pktToSend);
            sendto(sockfd, pktToSend, sizeof(Packet) + pkt_len, 0, addr, addr_length);
            decode(pktToSend);

            // Remove from inputBuffer (because it's now "sent")
            // but we do NOT free the packet, we keep it in an "unacked" list
            PacketNode* nextNode = node->next; // store so we don't lose it
            remove_buffer_packet(&inputBuffer, node);

            // Append it to "unackedList"
            // Packet* copied_node=(Packet*)malloc(sizeof(Packet)+pktToSend->length);
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
    }
}