#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <math.h>

#include "ionic.h"
#include "nbody.h"

/**
 * N-Body Simulation Node nanoservice implementation on RISC-V using IONIC GPR implementation.
 *
 * NN hdr format:
 *   bit<64> msg_type - 0 = Config, 1 = TraversalReq, 2 = TraversalResp
 *
 * Config Msg Format:
 *   bit<64> xcom
 *   bit<64> ycom
 *   bit<64> num_msgs
 *   bit<64> timestamp
 *
 * TraversalReq Format:
 *   bit<64> xpos
 *   bit<64> ypos
 *   bit<64> timestamp
 *
 * TraversalResp Format:
 *   bit<64> force
 *   bit<64> timestamp
 *
 */

int main(void) {
  // register context ID with IONIC
  ionic_add_context(0, 0);

  // local variables
  uint64_t app_hdr;
  uint64_t msg_type;
  uint64_t xcom, ycom;
  uint64_t xpos, ypos;
  int msg_cnt;
  uint64_t num_msgs;
  uint64_t force;
  uint64_t start_time;
  int configured;

  while(1) {
    msg_cnt = 0;
    configured = 0;
    // wait for a Config msg
    while (!configured) {
      ionic_wait();
      ionic_read(); // discard app hdr
      if (ionic_read() != CONFIG_TYPE) {
	printf("Expected Config msg.\n");
        return -1;
      }
      xcom = ionic_read();
      ycom = ionic_read();
      num_msgs = ionic_read();
      start_time = ionic_read();
      configured = 1;
    }
    // process all requests and send one response at the end
    while (msg_cnt < num_msgs) {
      ionic_wait();
      app_hdr = ionic_read(); // read app hdr
      if (ionic_read() != TRAVERSAL_REQ_TYPE) {
	printf("Expected TraversalReq msg.\n");
        return -1;
      }
      xpos = ionic_read();
      ypos = ionic_read();
      ionic_read(); // discard timestamp
      // compute force on the particle
      compute_force(xcom, ycom, xpos, ypos, &force);
      // send out TraversalResp
      ionic_write_r((app_hdr & (IP_MASK | CONTEXT_MASK)) | RESP_MSG_LEN);
      ionic_write_i(TRAVERSAL_RESP_TYPE);
      ionic_write_r(force);
      ionic_write_r(start_time);
      msg_cnt++;
    }
  }
  return 0;
}

