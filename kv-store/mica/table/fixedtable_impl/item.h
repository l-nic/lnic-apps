#pragma once

namespace mica {
namespace table {

template <class StaticConfig>
void FixedTable<StaticConfig>::set_item(Bucket *located_bucket,
                                        size_t item_index, ft_key_t key,
                                        const char *value) {
  located_bucket->key_arr[item_index] = key;

  uint8_t *_val = get_value(located_bucket, item_index);

  if (value == NULL) {
    uint64_t *_val64 = (uint64_t *)_val;
    _val64[0] = ionic_read();
    _val64[1] = ionic_read();
    _val64[2] = ionic_read();
    _val64[3] = ionic_read();
    _val64[4] = ionic_read();
    _val64[5] = ionic_read();
    _val64[6] = ionic_read();
    _val64[7] = ionic_read();
#if VALUE_SIZE_WORDS > 8
    _val64[8] = ionic_read();
    _val64[9] = ionic_read();
    _val64[10] = ionic_read();
    _val64[11] = ionic_read();
    _val64[12] = ionic_read();
    _val64[13] = ionic_read();
    _val64[14] = ionic_read();
    _val64[15] = ionic_read();
    _val64[16] = ionic_read();
    _val64[17] = ionic_read();
    _val64[18] = ionic_read();
    _val64[19] = ionic_read();
    _val64[20] = ionic_read();
    _val64[21] = ionic_read();
    _val64[22] = ionic_read();
    _val64[23] = ionic_read();
    _val64[24] = ionic_read();
    _val64[25] = ionic_read();
    _val64[26] = ionic_read();
    _val64[27] = ionic_read();
    _val64[28] = ionic_read();
    _val64[29] = ionic_read();
    _val64[30] = ionic_read();
    _val64[31] = ionic_read();
    _val64[32] = ionic_read();
    _val64[33] = ionic_read();
    _val64[34] = ionic_read();
    _val64[35] = ionic_read();
    _val64[36] = ionic_read();
    _val64[37] = ionic_read();
    _val64[38] = ionic_read();
    _val64[39] = ionic_read();
    _val64[40] = ionic_read();
    _val64[41] = ionic_read();
    _val64[42] = ionic_read();
    _val64[43] = ionic_read();
    _val64[44] = ionic_read();
    _val64[45] = ionic_read();
    _val64[46] = ionic_read();
    _val64[47] = ionic_read();
    _val64[48] = ionic_read();
    _val64[49] = ionic_read();
    _val64[50] = ionic_read();
    _val64[51] = ionic_read();
    _val64[52] = ionic_read();
    _val64[53] = ionic_read();
    _val64[54] = ionic_read();
    _val64[55] = ionic_read();
    _val64[56] = ionic_read();
    _val64[57] = ionic_read();
    _val64[58] = ionic_read();
    _val64[59] = ionic_read();
    _val64[60] = ionic_read();
    _val64[61] = ionic_read();
    _val64[62] = ionic_read();
    _val64[63] = ionic_read();
#endif // VALUE_SIZE_WORDS > 8
  }
  else {
    memcpy(_val, value, val_size);
  }
}
}
}
