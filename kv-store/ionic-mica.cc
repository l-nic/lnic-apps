#include <stdio.h>
#include <stdlib.h>
#include "ionic.h"

#define MICA_R_TYPE 1
#define MICA_W_TYPE 2

#define VALUE_SIZE_WORDS 64
#define KEY_SIZE_WORDS   2

#define SERVER_IP 0x0a000002
#define SERVER_CONTEXT 0

#define CLIENT_IP 0x0a000003
#define CLIENT_CONTEXT 1

#define PRINT_TIMING 0
#define NCORES 4
#define USE_ONE_CONTEXT 0
#define USE_MICA 1

// Expected address of the load generator
uint64_t load_gen_ip = 0x0a000001;

extern "C" {
    extern int _end;
    char* data_end = (char*)&_end + 16384*4;
    extern void sbrk_init(long int* init_addr);
    extern void __libc_init_array();
    extern void __libc_fini_array();
}


bool server_up = false;

typedef struct {
  volatile unsigned int lock;
} arch_spinlock_t;

#define arch_spin_is_locked(x) ((x)->lock != 0)

static inline void arch_spin_unlock(arch_spinlock_t *lock) {
  asm volatile (
    "amoswap.w.rl x0, x0, %0"
    : "=A" (lock->lock)
    :: "memory"
    );
}

static inline int arch_spin_trylock(arch_spinlock_t* lock) {
  int tmp = 1, busy;
  asm volatile (
    "amoswap.w.aq %0, %2, %1"
    : "=r"(busy), "+A" (lock->lock)
    : "r"(tmp)
    : "memory"
    );
  return !busy;
}

static inline void arch_spin_lock(arch_spinlock_t* lock) {
  while (1) {
    if (arch_spin_is_locked(lock)) {
      continue;
    }
    if (arch_spin_trylock(lock)) {
      break;
    }
  }
}

arch_spinlock_t up_lock;

#if USE_MICA

#define MICA_SHM_BUFFER_SIZE (8697016) // 10K 512B items
//#define MICA_SHM_BUFFER_SIZE (500040) // 500 512B items
//#define MICA_SHM_BUFFER_SIZE (63424) // 100 512B items
//#define MICA_SHM_BUFFER_SIZE (7928) // 10 512B items

#include "mica/table/fixedtable.h"

static constexpr size_t kValSize = VALUE_SIZE_WORDS * 8;

struct MyFixedTableConfig {
  static constexpr size_t kBucketCap = 15;

  // Support concurrent access. The actual concurrent access is enabled by
  // concurrent_read and concurrent_write in the configuration.
  static constexpr bool kConcurrent = false;

  // Be verbose.
  static constexpr bool kVerbose = false;

  // Collect fine-grained statistics accessible via print_stats() and
  // reset_stats().
  static constexpr bool kCollectStats = false;

  static constexpr size_t kKeySize = 8 * KEY_SIZE_WORDS;

  static constexpr bool concurrentRead = false;
  static constexpr bool concurrentWrite = false;

  static constexpr size_t itemCount = 10000;
};

typedef mica::table::FixedTable<MyFixedTableConfig> FixedTable;
typedef mica::table::Result MicaResult;

static inline uint64_t rotate(uint64_t val, int shift) {
  // Avoid shifting by 64: doing so yields an undefined result.
  return shift == 0 ? val : ((val >> shift) | (val << (64 - shift)));
}
static inline uint64_t HashLen16(uint64_t u, uint64_t v, uint64_t mul) {
  // Murmur-inspired hashing.
  uint64_t a = (u ^ v) * mul;
  a ^= (a >> 47);
  uint64_t b = (v ^ a) * mul;
  b ^= (b >> 47);
  b *= mul;
  return b;
}
// This was extracted from the cityhash library. It's the codepath for hashing
// 16 byte values.
static inline uint64_t cityhash(const uint64_t *s) {
  static const uint64_t k2 = 0x9ae16a3b2f90404fULL;
  uint64_t mul = k2 + (KEY_SIZE_WORDS * 8) * 2;
  uint64_t a = s[0] + k2;
  uint64_t b = s[1];
  uint64_t c = rotate(b, 37) * mul + a;
  uint64_t d = (rotate(a, 25) + b) * mul;
  return HashLen16(c, d, mul);
}

#endif // USE_MICA

int run_client(int cid) {
	uint64_t app_hdr;
  uint32_t src_ip;
  uint16_t src_context;
  uint16_t msg_len;
  uint16_t rx_msg_len;
  uint64_t start_time, stop_time;

  while (true) {
    arch_spin_lock(&up_lock);
    if (server_up) {
      arch_spin_unlock(&up_lock);
      break;
    } else {
      arch_spin_unlock(&up_lock);
      for (int k = 0; k < 100; k++) {
        asm volatile("nop");
      }
    }
  }

  printf("[%d] client starting.\n", cid);

  uint32_t dst_ip = SERVER_IP;
  uint16_t dst_context = SERVER_CONTEXT;

  uint64_t msg_val[VALUE_SIZE_WORDS];
  memset(msg_val, 0, VALUE_SIZE_WORDS*8);
  uint64_t msg_key[KEY_SIZE_WORDS];
  msg_key[1] = 0;
  msg_val[0] = 0x7;
  uint64_t ack;

#define NUM_ITERS 16
  for (int i = 0; i < NUM_ITERS; i++) {
    msg_key[0] = (i % 4) + 1;
    // SET
    msg_len = 8 + (8 * KEY_SIZE_WORDS) + (8 * VALUE_SIZE_WORDS);
    app_hdr = ((uint64_t)dst_ip << 32) | ((uint32_t)dst_context << 16) | msg_len;
    ionic_write_r(app_hdr);
    ionic_write_i(MICA_W_TYPE);
    ionic_write_r(msg_key[0]);
    ionic_write_r(msg_key[1]);
    ionic_write_r(msg_val[0]);
    ionic_write_r(msg_val[1]);
    ionic_write_r(msg_val[2]);
    ionic_write_r(msg_val[3]);
    ionic_write_r(msg_val[4]);
    ionic_write_r(msg_val[5]);
    ionic_write_r(msg_val[6]);
    ionic_write_r(msg_val[7]);
    ionic_write_r(msg_val[8]);
    ionic_write_r(msg_val[9]);
    ionic_write_r(msg_val[10]);
    ionic_write_r(msg_val[11]);
    ionic_write_r(msg_val[12]);
    ionic_write_r(msg_val[13]);
    ionic_write_r(msg_val[14]);
    ionic_write_r(msg_val[15]);
    ionic_write_r(msg_val[16]);
    ionic_write_r(msg_val[17]);
    ionic_write_r(msg_val[18]);
    ionic_write_r(msg_val[19]);
    ionic_write_r(msg_val[20]);
    ionic_write_r(msg_val[21]);
    ionic_write_r(msg_val[22]);
    ionic_write_r(msg_val[23]);
    ionic_write_r(msg_val[24]);
    ionic_write_r(msg_val[25]);
    ionic_write_r(msg_val[26]);
    ionic_write_r(msg_val[27]);
    ionic_write_r(msg_val[28]);
    ionic_write_r(msg_val[29]);
    ionic_write_r(msg_val[30]);
    ionic_write_r(msg_val[31]);
    ionic_write_r(msg_val[32]);
    ionic_write_r(msg_val[33]);
    ionic_write_r(msg_val[34]);
    ionic_write_r(msg_val[35]);
    ionic_write_r(msg_val[36]);
    ionic_write_r(msg_val[37]);
    ionic_write_r(msg_val[38]);
    ionic_write_r(msg_val[39]);
    ionic_write_r(msg_val[40]);
    ionic_write_r(msg_val[41]);
    ionic_write_r(msg_val[42]);
    ionic_write_r(msg_val[43]);
    ionic_write_r(msg_val[44]);
    ionic_write_r(msg_val[45]);
    ionic_write_r(msg_val[46]);
    ionic_write_r(msg_val[47]);
    ionic_write_r(msg_val[48]);
    ionic_write_r(msg_val[49]);
    ionic_write_r(msg_val[50]);
    ionic_write_r(msg_val[51]);
    ionic_write_r(msg_val[52]);
    ionic_write_r(msg_val[53]);
    ionic_write_r(msg_val[54]);
    ionic_write_r(msg_val[55]);
    ionic_write_r(msg_val[56]);
    ionic_write_r(msg_val[57]);
    ionic_write_r(msg_val[58]);
    ionic_write_r(msg_val[59]);
    ionic_write_r(msg_val[60]);
    ionic_write_r(msg_val[61]);
    ionic_write_r(msg_val[62]);
    ionic_write_r(msg_val[63]);
    start_time = rdcycle();
    ionic_wait();
    stop_time = rdcycle();
    app_hdr = ionic_read();
    src_ip = (app_hdr & IP_MASK) >> 32;
    src_context = (app_hdr & CONTEXT_MASK) >> 16;
    rx_msg_len = app_hdr & LEN_MASK;
    if (src_ip != SERVER_IP) printf("CLIENT ERROR: Expected: correct_sender_ip = %x, Received: src_ip = %x\n", SERVER_IP, src_ip);
    if (src_context != SERVER_CONTEXT) printf("CLIENT ERROR: Expected: correct_src_context = %d, Received: src_context = %d\n", SERVER_CONTEXT, src_context);
    if (rx_msg_len != 8) printf("CLIENT ERROR: Expected: msg_len = %d, Received: msg_len = %d\n", 8, rx_msg_len);
    ack = ionic_read();
    if (ack != 0x1) printf("CLIENT ERROR: Expected: ack = 0x%x, Received: ack = 0x%lx\n", 0x1, ack);
    ionic_msg_done();
    printf("[%d] client SET key=%lx val=%lx. Latency: %ld\n", cid, msg_key[0], msg_val[0], stop_time-start_time);

    // GET
    app_hdr = ((uint64_t)dst_ip << 32) | ((uint32_t)dst_context << 16) | (8 + (8 * KEY_SIZE_WORDS));
    ionic_write_r(app_hdr);
    ionic_write_i(MICA_R_TYPE);
    ionic_write_r(msg_key[0]);
    ionic_write_r(msg_key[1]);
    start_time = rdcycle();
    ionic_wait();
    stop_time = rdcycle();
    app_hdr = ionic_read();
    src_ip = (app_hdr & IP_MASK) >> 32;
    src_context = (app_hdr & CONTEXT_MASK) >> 16;
    rx_msg_len = app_hdr & LEN_MASK;
    if (src_ip != SERVER_IP) printf("CLIENT ERROR: Expected: correct_sender_ip = %x, Received: src_ip = %x\n", SERVER_IP, src_ip);
    if (src_context != SERVER_CONTEXT) printf("CLIENT ERROR: Expected: correct_src_context = %d, Received: src_context = %d\n", SERVER_CONTEXT, src_context);
    if (rx_msg_len != 8*VALUE_SIZE_WORDS) printf("CLIENT ERROR: Expected: msg_len = %d, Received: msg_len = %d\n", 8*VALUE_SIZE_WORDS, rx_msg_len);
    msg_val[0] = ionic_read();
    for (int j = 1; j < VALUE_SIZE_WORDS; j++)
      ionic_read();
    if (msg_val[0] != 0x7) printf("CLIENT ERROR: Expected: msg_val = 0x%x, Received: msg_val[0] = 0x%lx\n", 0x7, msg_val[0]);
    ionic_msg_done();
    printf("[%d] client GET key=%lx val=%lx. Latency: %ld\n", cid, msg_key[0], msg_val[0], stop_time-start_time);
  }

  return EXIT_SUCCESS;
}

void send_startup_msg(int cid, uint64_t context_id) {
  uint64_t app_hdr = (load_gen_ip << 32) | (0 << 16) | (2*8);
  uint64_t cid_to_send = cid;
  ionic_write_r(app_hdr);
  ionic_write_r(cid_to_send);
  ionic_write_r(context_id);
}


int run_server(int cid, uint64_t context_id) {
	uint64_t app_hdr;
  uint64_t msg_type;
#if PRINT_TIMING
  uint64_t t0, t1, t2, t3;
#endif // PRINT_TIMING
  uint64_t service_time, sent_time;
#if USE_MICA
  uint64_t key_hash;
  MicaResult out_result;
  FixedTable::ft_key_t ft_key;
  FixedTable table(kValSize, cid);
#endif // USE_MICA

  uint64_t init_value[VALUE_SIZE_WORDS];
  memset(init_value, 0, VALUE_SIZE_WORDS*8);

  printf("[%d] Inserting keys from %ld to %ld.\n", cid, (MyFixedTableConfig::itemCount * context_id) + 1, (MyFixedTableConfig::itemCount * context_id) + MyFixedTableConfig::itemCount);
  for (unsigned i = (MyFixedTableConfig::itemCount * context_id) + 1;
      i <= (MyFixedTableConfig::itemCount * context_id) + MyFixedTableConfig::itemCount; i++) {
    ft_key.qword[0] = i;
    ft_key.qword[1] = 0;
    init_value[0] = i;
    init_value[1] = i + 1;
    init_value[2] = i + 2;
    key_hash = cityhash(ft_key.qword);
    out_result = table.set(key_hash, ft_key, reinterpret_cast<char *>(init_value));
    if (out_result != MicaResult::kSuccess) printf("[%d] Inserting key %lu failed (%s).\n", cid, ft_key.qword[0], mica::table::cResultString(out_result));
    if (i % 100 == 0) printf("[%d] Inserted keys up to %d.\n", cid, i);
  }

  printf("[%d] Server listenning on context_id=%ld.\n", cid, context_id);
  arch_spin_lock(&up_lock);
  server_up = true;
  arch_spin_unlock(&up_lock);

  send_startup_msg(cid, context_id);

  while (1) {
    ionic_wait();
#if PRINT_TIMING
    t0 = rdcycle();
#endif // PRINT_TIMING
    app_hdr = ionic_read();
    //printf("[%d] --> Received msg of length: %u bytes\n", cid, (uint16_t)app_hdr);
    service_time = ionic_read();
    sent_time = ionic_read();
    msg_type = ionic_read();
    ft_key.qword[0] = ionic_read();
    ft_key.qword[1] = ionic_read();

#if PRINT_TIMING
    t1 = rdcycle();
#endif // PRINT_TIMING
#if USE_MICA
    //key_hash = cityhash(ft_key.qword);
    key_hash = ionic_read();
#endif // USE_MICA
#if PRINT_TIMING
    t2 = rdcycle();
#endif // PRINT_TIMING

    if (msg_type == MICA_R_TYPE) {
      ionic_write_r((app_hdr & (IP_MASK | CONTEXT_MASK)) | (8 + 8 + (8 * VALUE_SIZE_WORDS)));
      ionic_write_r(service_time);
      ionic_write_r(sent_time);
#if USE_MICA
      // XXX We don't pass a pointer to the value, because we moved ionic_write() into MICA:
      out_result = table.get(key_hash, ft_key, NULL);
      if (out_result != MicaResult::kSuccess) {
        printf("[%d] GET failed for key %lu.\n", cid, ft_key.qword[0]);
      }
#endif // USE_MICA
    }
    else {
#if USE_MICA
      // XXX We don't pass a pointer to the value, because we moved ionic_read() into MICA:
      out_result = table.set(key_hash, ft_key, NULL);
      if (out_result != MicaResult::kSuccess) {
        printf("[%d] Inserting key %lu failed.\n", cid, ft_key.qword[0]);
      }
#endif // USE_MICA
      ionic_write_r((app_hdr & (IP_MASK | CONTEXT_MASK)) | (8 + 8 + 8));
      ionic_write_r(service_time);
      ionic_write_r(sent_time);
      ionic_write_i(0x1); // ACK
    }

    ionic_msg_done();
#if PRINT_TIMING
    t3 = rdcycle();
    printf("[%d] %s key=%lu. Hash lat: %ld     MICA latency: %ld     Total latency: %ld\n", cid,
        msg_type == MICA_R_TYPE ? "GET" : "SET", ft_key.qword[0], t2-t1, t3-t2, t3-t0);
#endif // PRINT_TIMING
	}

  return EXIT_SUCCESS;
}

extern "C" {
bool is_single_core() { return false; }

int core_main(int argc, char** argv, int cid, int nc) {
  (void)argc; (void)argv; (void)nc;
  uint64_t priority = 0;
  uint64_t context_id = USE_ONE_CONTEXT ? 0 : cid;

  if (cid > (NCORES-1)) {
    //ionic_add_context(context_id, priority);
    //send_startup_msg(cid, context_id);
    return EXIT_SUCCESS;
  }
  else {
    ionic_add_context(context_id, priority);
  }

  if (cid == 0) {
    // Setup the C++ libraries
    sbrk_init((long int*)data_end);
    atexit(__libc_fini_array);
    __libc_init_array();

    printf("Each core serving %ld items.\n", MyFixedTableConfig::itemCount);
  }

  int ret = run_server(cid, context_id);

  return ret;
}

}
