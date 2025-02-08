#include <arpa/inet.h>
#include <stdbool.h>
#include <stdint.h>

//2/ Basic reliability: say stop-and-wait. You need to implement the 
//  sending queue, timers to schedule retransmissions, 
//  and ACKs. Try transmitting files between your client and your server.

#define MSS 1012 // MSS = Maximum Segment Size (aka max length)
#define ACK
#define DATA
#define ACKDATA
#define QUEUE_SIZE 10


typedef struct {
    uint16_t seq; 
    uint16_t ack;
    uint16_t length;  
    uint16_t window;
    uint16_t flags; 
    uint16_t unused;
    uint8_t payload[0];  
} packet;

typedef struct {
    packet packets[QUEUE_SIZE];
    int front;                   
    int rear;                    
    int count;                   
} pQueue;

void init_queue(pQueue *q) {
    q->front = 0;
    q->rear = 0;
    q->count = 0;
}

int enqueue(pQueue *q, packet *pkt) {
    if (q->count == QUEUE_SIZE) {
        printf("queue is already full\n");
        return -1; 
    }

    q->packets[q->rear] = *pkt; 
    q->rear = (q->rear + 1) % QUEUE_SIZE;
    q->count++;
    return 0;
}

int check_queue(pQueue *q, uint16_t seq) {
    for (int i; i < q->count; i++) {
        packet *pkt = &q->packets[i];
        if (pkt->seq < seq) {
            dequeue(q, pkt);
        }
    }
}

int dequeue(pQueue *q, packet *pkt) {
    if (q->count == 0) {
        printf("queue is empty\n");
        return -1; 
    }

    *pkt = q->packets[q->front];
    q->front = (q->front + 1) % QUEUE_SIZE;
    q->count--;
    return 0;
}

int send_packet(int sockfd, struct sockaddr_in *dest_addr, pQueue *q) {
    if (q->count == 0) {
        printf("no packets in queue\n");
        return -1;
    }

    packet pkt;
    if (dequeue(q, &pkt) == 0) {
        int sent_bytes = sendto(sockfd, &pkt, sizeof(packet), 0,
                                (struct sockaddr *) dest_addr, sizeof(*dest_addr));
        if (sent_bytes < 0) {
            perror("sendto failed");
            return -1;
        }
        printf("sent packet with seq: %d\n", ntohs(pkt.seq));
    }

    return 0;
}

// Main function of transport layer; never quits
void listen_loop(int sockfd, struct sockaddr_in* addr, int type,
                 ssize_t (input_p)(uint8_t, size_t),
                 void (output_p)(uint8_t, size_t)) {

    // We should have a starting sequence number for server at this step
    uint16_t seq = rand(); 

    //set up packet queue
    pQueue queue;
    init_queue(&queue);


    while (true) {
        // Assume the socket has been set up with all other variables
        char buf[sizeof(packet) + MSS] = {0};
        packet* pkt = (packet*) buf;
        int bytes_recvd = recvfrom(sockfd, pkt, sizeof(packet) + MSS, 0, (struct sockaddr*) &server_addr, &s);

        uint16_t seq = ntohs(pkt->seq); // Make sure to convert to little endian
        uint16_t flags = ntohs(pkt->flags);
        
        // 1 read in the received packet (3 scenarios: 1. Ack packet 2. Data packet 3. Ack+Data packet)
        if (flags == ACK){ // a. if ack packet, 
            // 1. remove send buffer packets less than new ack packet seq number. (size of current window reduce)
            check_queue(&queue, seq);
        } else if (flags == DATA) // b. if data packet,
        {
            // 1. If there is available buffer inside receive buffer, store the packet inside receive buffer, else discard.
                    // If the newly arrived packet have lowest seq number in current window, write the stream formed by the current lowest
                    // numbered packet into the standard output, 
        }else if (flags == ACKDATA) // c. if ack+data packet
        {
            // 1. remove send buffer packets less than new ack packet seq number. (size of current window reduce)
                // 2.If there is available buffer inside receive buffer, store the packet inside receive buffer, else discard.
                    // If the newly arrived packet have lowest seq number in current window, write the stream formed by the current lowest
                    // numbered packet into the standard output, 
        }
        // 2 read all standard input 
            // a. parse the standard input into multiple packet
            // b. save the input packet into send buffer
            // c. Compute the number of packets that I can send (available in receiver's receive buffer) (Flow window size from other side - size of current window)
            // d. send the packets I can send to receiver. 
    }
}