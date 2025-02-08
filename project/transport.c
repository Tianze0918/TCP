#include <arpa/inet.h>
#include <stdbool.h>
#include <stdint.h>
#include <errno.h>
#include <cstddef>
#include <cstdlib>
#define MSS 1012 // MSS = Maximum Segment Size (aka max length)

typedef struct Packet{
    uint16_t seq; 
    uint16_t ack;
    uint16_t length;  
    uint16_t window;
    uint16_t flags; 
    uint16_t unused;
    uint8_t payload[0];  
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
    }
    return NULL;
}

PacketNode* remove(PacketList* list, PacketNode* node){
    if (node==list->head){
        list->head=node->next;
        list->head->previous=NULL;
        free(node);
    }else if(node==list->head){
        list->tail=node->previous;
        list->tail->next=NULL;
        free(node);
    }else{
        node->previous->next=node->next;
        node->next->previous=node->previous;
        free(node);
    }
}

void output(PacketList* list, ssize_t *write_seq, ssize_t* receiver_flowWindow, Packet* pkt, void (*output_p)(uint8_t*, size_t)){
   while (true){
        PacketNode* found=find(list, *write_seq);
        if (found!=NULL){
            // updating next ack seq for receiver
            *write_seq+=1;
            // Receiver side Flow window update
            ssize_t newFlowWindow=ntohs(pkt->window);
            *receiver_flowWindow = (newFlowWindow) > *receiver_flowWindow  ? newFlowWindow : receiver_flowWindow;
            output_p(found->pkt->payload, found->pkt->length);
            remove(list, found);
        }else{
            return;
        }
   }
}


// Enqueue (append) a packet pointer to the list.
void enqueuePacket(PacketList* list, Packet* pkt) {
    PacketNode* node = (PacketNode*)malloc(sizeof(PacketNode));
    if (!node) {
        perror("malloc for PacketNode");
        exit(EXIT_FAILURE);
    }
    node->pkt = pkt;
    node->next = NULL;
    
    if (list->tail == NULL) {  // List is empty
        list->head = node;
        list->tail = node;
    } else {
        list->tail->next = node;
        list->tail = node;
    }
}

void update_input(PacketList* list, Packet* pkt, ssize_t* inputSeq, ssize_t* currentWindowSize){
    while (inputSeq < pkt->ack){
        PacketNode* found=find(list, *inputSeq);
        if (found!=NULL){
            *inputSeq+=1;
            ssize_t packet_length=ntohs(found->pkt->length);
            *currentWindowSize-=packet_length;
            remove(list, found);
        }else{
            return;
        }
    }
    
}

void set_header(Packet* pkt, ssize_t seq, ssize_t ack, ssize_t length, ssize_t flow_window, ssize_t flags){
    pkt->seq=seq;
    pkt->ack=ack;
    pkt->length=length;
    pkt->window=flow_window;
    pkt->flags=flags;
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

    ssize_t inputSeq;
    ssize_t currentWindowEnd;
    ssize_t outputSeq;


    // socket address struct length used by recvfrom
    socklen_t addr_length=sizeof (*addr);


    // Input buffer to store stdin
    uint8_t buffer[MSS]={0};
    ssize_t bytes_read;



    while (true){
        // Place stdin into input buffer
        char buf[sizeof(Packet) + MSS] = {0};
        Packet* pkt = (Packet*) buf;

        while ((bytes_read=input_p(buffer , MSS)) > 0){
            memcpy(pkt->payload, buffer, bytes_read);
            enqueuePacket(&inputBuffer, pkt);
        }

        //Receiving part
        while ((bytes_read = recvfrom(sockfd, pkt, sizeof(pkt) + MSS, 0, (struct sockaddr*) addr, &addr_length) != -1)) {
            //place arriving packets into the output buffer
            enqueuePacket(&outputBuffer, pkt);
            //1. print packets to standard output
            //2. update the ack number (outputSeq)
            //3. update the flow window
            output(&outputBuffer, &outputSeq, &flowWindow, pkt, output_p);
            //clear input buffer if the ariving packet is ack
            update_input(&inputBuffer, pkt, inputSeq, &currentWindowSize);
        }



        //Sending part
        PacketNode* next_packet_node=find(&inputBuffer, currentWindowEnd+1);
        if (next_packet_node==NULL){ //no more packets to send
            continue;
        }else{
            Packet* next_packet=next_packet_node->pkt;
            ssize_t packet_length=ntohs(next_packet->length);
            while (currentWindowSize + packet_length < flowWindow){
                uint16_t flags; //flags setting required
                set_header(next_packet, currentWindowEnd, outputSeq, sizeeof(next_packet), currentWindowSize, flags);
                sendto(sockfd, next_packet, sizeof(next_packet), 0, addr, addr_length);
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
    while (true) {
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
    }
}
