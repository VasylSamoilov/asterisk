/*
 * SPDX-License-Identifier: GPL-2.0-or-later
 *
 * mbl_read()/mbl_write() SCO framing harness on real SOCK_SEQPACKET sockets,
 * modelled on the seqpacket test in asterisk-iphone-bluetooth-gateway-lab
 * (Borisych-C). Run through run-tests.py, which extracts the functions
 * into sco_functions.inc.
 */
#include <assert.h>
#include <errno.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>
#include <poll.h>
#define DEVICE_FRAME_SIZE_DEFAULT 48
#define DEVICE_FRAME_SIZE_MAX 256
#define SCO_SIZE_CHANGE_COUNT 3
#define CHANNEL_FRAME_SIZE 80
#define AST_FRIENDLY_OFFSET 64
#define AST_FRAME_VOICE 1
#define DEVICE_FRAME_FORMAT 1
#define ast_debug(...) ((void)0)
#define ast_log(...) ((void)0)
#define ast_mutex_trylock(...) 0
#define ast_mutex_unlock(...) ((void)0)
#define CHANNEL_DEADLOCK_AVOIDANCE(...) ((void)0)
static int fd_set_to = 99;
#define ast_channel_set_fd(c,i,f) (fd_set_to=(f))
struct ast_frame {
    int frametype, datalen, samples, offset, mallocd;
    struct { int format; } subclass;
    const char *src;
    struct { long tv_sec, tv_usec; } delivery;
    union { void *ptr; } data;
};
static struct ast_frame ast_null_frame;
struct ast_smoother { unsigned size, used; unsigned char data[4096], output[320]; struct ast_frame frame; };
static void ast_smoother_reset(struct ast_smoother *s, unsigned n)
{ assert(n && n <= 256 && !(n%2)); s->size=n; s->used=0; }
static void ast_smoother_feed(struct ast_smoother *s, const struct ast_frame *f)
{ assert(s->used+f->datalen<=sizeof(s->data)); memcpy(s->data+s->used,f->data.ptr,f->datalen); s->used+=f->datalen; }
static struct ast_frame *ast_smoother_read(struct ast_smoother *s)
{
    if (s->used<s->size) return NULL;
    memcpy(s->output,s->data,s->size); s->used-=s->size; memmove(s->data,s->data+s->size,s->used);
    s->frame.data.ptr=s->output; s->frame.datalen=s->size; return &s->frame;
}
struct mbl_pvt {
    void *owner; struct ast_frame fr; int lock, sco_socket, do_alignment_detection, dsp;
    int sco_mtu, sco_tx_size, sco_rx_candidate, sco_rx_candidate_count; unsigned sco_in_reset:1; char id[32], io_buf[DEVICE_FRAME_SIZE_MAX+AST_FRIENDLY_OFFSET];
    struct ast_smoother *bt_in_smoother, *bt_out_smoother;
};
struct ast_channel { struct mbl_pvt *pvt; };
#define ast_channel_tech_pvt(c) ((c)->pvt)
static void do_alignment_detection(struct mbl_pvt *p, void *b, int n) { (void)p; (void)b; (void)n; }
static struct ast_frame *ast_dsp_process(struct ast_channel *c, int d, struct ast_frame *f) { (void)c; (void)d; return f; }
#include "sco_functions.inc"

static unsigned char bytes[2048], got[512];
static struct ast_smoother in, out;
static struct mbl_pvt p = { .bt_in_smoother=&in, .bt_out_smoother=&out };
static struct ast_channel chan = { .pvt=&p };
static int peer;
static void new_socket(int mtu) {
    int pair[2]; assert(!socketpair(AF_UNIX,SOCK_SEQPACKET,0,pair));
    p.sco_socket=pair[0]; peer=pair[1]; sco_reset_audio(&p, mtu);
}
static struct ast_frame v320 = { .frametype=AST_FRAME_VOICE, .datalen=320, .samples=160 };

static void check_size(int pkt, int mtu, int expect_tx) {
    new_socket(mtu);
    assert(mbl_read(&chan)==&ast_null_frame); /* EAGAIN; applies deferred reset */
    assert(!in.used && !out.used && p.sco_tx_size==0);
    mbl_write(&chan,&v320);   /* no TX before first RX */
    assert(recv(peer,got,sizeof(got),MSG_DONTWAIT)==-1 && errno==EAGAIN);
    int frames=0;
    for (int i=0;i<10;i++) {
        assert(send(peer,bytes,pkt,0)==pkt);
        struct ast_frame *f=mbl_read(&chan);
        assert(f);
        if (f!=&ast_null_frame) { assert(f->datalen==CHANNEL_FRAME_SIZE); frames++; }
    }
    assert(frames >= 1);
    assert(p.sco_tx_size==expect_tx);
    mbl_write(&chan,&v320);
    for (int i=0;i<320/expect_tx;i++) {
        assert(recv(peer,got,sizeof(got),MSG_DONTWAIT)==expect_tx);
        assert(!memcmp(got,bytes+i*expect_tx,expect_tx));
    }
    assert(recv(peer,got,sizeof(got),MSG_DONTWAIT)==-1);
    /* partial input + EAGAIN -> null frame, not NULL */
    assert(send(peer,bytes,2,0)==2);
    assert(mbl_read(&chan)!=NULL);
    { struct ast_frame *f; int n=0; while ((f=mbl_read(&chan))!=&ast_null_frame) { assert(f && ++n<20); } }
    assert(p.sco_socket!=-1); /* EAGAIN: audio kept */
    assert(p.sco_tx_size==expect_tx); /* stray short block did not reframe */
    /* odd block ignored */
    assert(send(peer,bytes,7,0)==7);
    assert(mbl_read(&chan)==&ast_null_frame && p.sco_tx_size==expect_tx);
    { struct ast_frame *f; while ((f=mbl_read(&chan))!=&ast_null_frame) assert(f); }
    /* EOF closes audio only, returns null frame, no spin */
    close(peer);
    assert(mbl_read(&chan)==&ast_null_frame);
    assert(p.sco_socket==-1 && p.sco_tx_size==0 && fd_set_to==-1 && !in.used);
    mbl_write(&chan,&v320); /* no socket: no crash */
}

int main(void) {
    p.owner=&p;
    for (unsigned i=0;i<sizeof(bytes);i++) bytes[i]=(unsigned char)(i*7+1);
    v320.data.ptr=bytes;
    check_size(48,255,48);   /* iPhone16/Realtek: radio 60, host 48 */
    check_size(60,255,60);
    check_size(96,255,96);
    check_size(120,255,120);
    check_size(60,0,60);     /* MTU unknown */
    check_size(48,31,30);    /* MTU bound, kept even */
    check_size(240,255,240);
    /* size change mid-call drops partial TX */
    new_socket(255);
    send(peer,bytes,48,0); mbl_read(&chan); assert(p.sco_tx_size==48);
    struct ast_frame v40={.frametype=AST_FRAME_VOICE,.datalen=40,.samples=20,.data.ptr=bytes};
    mbl_write(&chan,&v40); assert(out.used==40);
    send(peer,bytes,96,0); mbl_read(&chan); assert(p.sco_tx_size==48 && out.used==40); /* one stray block ignored */
    send(peer,bytes,48,0); mbl_read(&chan);
    send(peer,bytes,96,0); mbl_read(&chan); send(peer,bytes,96,0); mbl_read(&chan); assert(p.sco_tx_size==48);
    send(peer,bytes,96,0); mbl_read(&chan); assert(p.sco_tx_size==96 && out.used==0); /* 3 in a row switches */
    /* socket replacement clears stale state */
    send(peer,bytes,30,0); mbl_read(&chan); if (!in.used) { send(peer,bytes,30,0); mbl_read(&chan); } assert(in.used);
    close(peer); close(p.sco_socket);
    new_socket(255); assert(in.used && p.sco_in_reset); /* deferred */
    assert(mbl_read(&chan)==&ast_null_frame); assert(!in.used && !out.used && !p.sco_tx_size);
    /* oversize MTU is ignored as a bound */
    sco_reset_audio(&p, 1000); assert(p.sco_mtu==0);
    /* RX backlog: big blocks, poll-driven reads (one call per readable fd) */
    int sizes[]={96,240};
    for (int k=0;k<2;k++) {
        close(peer); close(p.sco_socket); new_socket(255);
        long in_bytes=0, out_bytes=0;
        for (int i=0;i<600;i++) {
            assert(send(peer,bytes,sizes[k],0)==sizes[k]); in_bytes+=sizes[k];
            /* asterisk keeps calling read while data or buffered frames exist */
            /* asterisk reads only when poll() reports the fd readable */
            struct pollfd pfd={.fd=p.sco_socket,.events=POLLIN};
            while (poll(&pfd,1,0)==1) {
                struct ast_frame *f=mbl_read(&chan); assert(f);
                if (f!=&ast_null_frame) out_bytes+=f->datalen;
            }
            /* also one 5ms timer-driven read per 80 bytes of channel time */
            assert((int)in.used < sizes[k]);
        }
        assert(in_bytes-out_bytes < sizes[k]);
    }
    puts("PASS");
    return 0;
}
