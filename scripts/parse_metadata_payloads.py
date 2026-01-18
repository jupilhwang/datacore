#!/usr/bin/env python3
from pathlib import Path

def load_hex(path):
    s = Path(path).read_text().strip()
    return bytes.fromhex(s)

def uvar(data, start):
    res=0; shift=0; i=start
    while True:
        if i>=len(data): raise EOFError('uvar EOF')
        b=data[i]
        res |= (b & 0x7f) << shift
        i+=1
        if b & 0x80 == 0: break
        shift +=7
    return res, i-start

def parse_metadata(payload):
    out=[]
    def add(line): out.append(line)
    add('total payload bytes: %d' % len(payload))
    add('raw_payload_hex: ' + payload.hex())
    if len(payload) < 8:
        add('payload too small')
        return out
    size = int.from_bytes(payload[0:4],'big',signed=True)
    corr = int.from_bytes(payload[4:8],'big',signed=True)
    add('offset 0:  %08x -> frame_size (int32) = %d' % (0,size))
    add('offset 4:  %08x -> correlation_id (int32) = %d' % (4,corr))
    off = 8
    add('\n-- body parsing (offsets are absolute from payload start) --')
    try:
        # throttle_time_ms
        thr = int.from_bytes(payload[off:off+4],'big',signed=True)
        add('offset %03d: %s -> throttle_time_ms = %d' % (off, payload[off:off+4].hex(), thr))
        off+=4
        # brokers
        val,cons = uvar(payload, off)
        add('offset %03d: %s -> brokers_compact_array_len(uvarint) = %d -> count=%d' % (off, payload[off:off+cons].hex(), val, val-1))
        off += cons
        bcount = val-1
        for bi in range(bcount):
            add('\n-- broker %d --' % bi)
            node = int.from_bytes(payload[off:off+4],'big',signed=True)
            add('offset %03d: %s -> broker.node_id = %d' % (off, payload[off:off+4].hex(), node)); off+=4
            v,cons = uvar(payload, off)
            add('offset %03d: %s -> host_compact_string_len(uvarint)=%d -> len=%d' % (off, payload[off:off+cons].hex(), v, v-1)); off += cons
            if v-1>0:
                add('offset %03d: %s -> host bytes = %s' % (off, payload[off:off+v-1].hex(), payload[off:off+v-1].decode(errors='replace')))
                off += v-1
            port = int.from_bytes(payload[off:off+4],'big',signed=True)
            add('offset %03d: %s -> broker.port = %d' % (off, payload[off:off+4].hex(), port)); off+=4
            v,cons = uvar(payload, off)
            add('offset %03d: %s -> rack_compact_nullable_string uvarint = %d' % (off, payload[off:off+cons].hex(), v)); off += cons
            if v!=0:
                add('offset %03d: %s -> rack bytes = %s' % (off, payload[off:off+v-1].hex(), payload[off:off+v-1].decode(errors='replace'))); off += v-1
            tf_count,cons = uvar(payload, off)
            add('offset %03d: %s -> broker.tagged_fields count (uvarint) = %d' % (off, payload[off:off+cons].hex(), tf_count)); off += cons
            for t in range(tf_count):
                tag,cl = uvar(payload, off); off+=cl
                size,cl = uvar(payload, off); off+=cl
                data = payload[off:off+size]; off += size
                add(' offset %03d: tag=%d size=%d data_hex=%s' % (off-size, tag, size, data.hex()))
        # cluster_id
        v,cons = uvar(payload, off)
        add('\noffset %03d: %s -> cluster_id_compact_nullable uvarint=%d' % (off, payload[off:off+cons].hex(), v)); off += cons
        if v!=0:
            add('offset %03d: %s -> cluster_id = %s' % (off, payload[off:off+v-1].hex(), payload[off:off+v-1].decode(errors='replace'))); off += v-1
        # controller id
        ctrl = int.from_bytes(payload[off:off+4],'big',signed=True)
        add('offset %03d: %s -> controller_id = %d' % (off, payload[off:off+4].hex(), ctrl)); off += 4
        # topics
        v,cons = uvar(payload, off)
        add('offset %03d: %s -> topics_compact_array_len(uvarint)=%d -> count=%d' % (off, payload[off:off+cons].hex(), v, v-1)); off += cons
        tcount = v-1
        for ti in range(tcount):
            add('\n-- topic %d --' % ti)
            err = int.from_bytes(payload[off:off+2],'big',signed=True); add('offset %03d: %s -> topic.error_code = %d' % (off, payload[off:off+2].hex(), err)); off += 2
            vv,cl = uvar(payload, off); add('offset %03d: %s -> topic.name_compact_len(uvarint)=%d -> len=%d' % (off, payload[off:off+cl].hex(), vv, vv-1)); off += cl
            if vv-1>0:
                add('offset %03d: %s -> topic.name = %s' % (off, payload[off:off+vv-1].hex(), payload[off:off+vv-1].decode(errors='replace'))); off += vv-1
            # topic_id 16 bytes
            add('offset %03d: %s -> topic_id (16 bytes) = %s' % (off, payload[off:off+16].hex(), payload[off:off+16].hex())); off += 16
            add('offset %03d: %s -> is_internal = %d' % (off, payload[off:off+1].hex(), payload[off]))
            off += 1
            pv,cl = uvar(payload, off); add('offset %03d: %s -> partitions_compact_array_len=%d -> count=%d' % (off, payload[off:off+cl].hex(), pv, pv-1)); off += cl
            pcount = pv-1
            for pi in range(pcount):
                perr = int.from_bytes(payload[off:off+2],'big',signed=True); off+=2
                pidx = int.from_bytes(payload[off:off+4],'big',signed=True); off+=4
                leader = int.from_bytes(payload[off:off+4],'big',signed=True); off+=4
                add('offset %03d: partition.index=%d leader=%d error=%d' % (off-10, pidx, leader, perr))
                rv,cl = uvar(payload, off); add('offset %03d: replicas_compact_len uvar=%d -> count=%d' % (off, rv, rv-1)); off+=cl
                for _ in range(rv-1): off+=4
                iv,cl = uvar(payload, off); add('offset %03d: isr_compact_len uvar=%d -> count=%d' % (off, iv, iv-1)); off+=cl
                for _ in range(iv-1): off+=4
                ov,cl = uvar(payload, off); add('offset %03d: offline_replicas_compact_len uvar=%d -> count=%d' % (off, ov, ov-1)); off+=cl
                for _ in range(ov-1): off+=4
                tfc,cl = uvar(payload, off); add('offset %03d: partitions.tagged_fields_count=%d' % (off, tfc)); off+=cl
                for _ in range(tfc):
                    tag,cl2 = uvar(payload, off); off+=cl2
                    size,cl2 = uvar(payload, off); off+=cl2
                    off += size
    except Exception as e:
        add('\nparse error during body parsing: %s' % str(e))
        return out

    try:
        tfroot,cl = uvar(payload, off)
        add('\noffset %03d: %s -> root.tagged_fields_count(uvarint)=%d' % (off, payload[off:off+cl].hex(), tfroot))
    except Exception:
        add('\noffset %03d: (no root tagged fields readable)' % off)
    return out

if __name__=='__main__':
    import sys
    args = sys.argv[1:]
    if len(args) == 0:
        p1 = parse_metadata(load_hex('/tmp/pkt233_payload_hex.txt'))
        p2 = parse_metadata(load_hex('/tmp/pkt119_payload_hex.txt'))
        Path('src/metadata_packet1_map.txt').write_text('\n'.join(p1))
        Path('src/metadata_packet2_map.txt').write_text('\n'.join(p2))
        print('wrote src/metadata_packet1_map.txt and src/metadata_packet2_map.txt')
    elif len(args) == 1:
        p = parse_metadata(load_hex(args[0]))
        outp = 'src/metadata_packet_map.txt'
        Path(outp).write_text('\n'.join(p))
        print('wrote', outp)
    else:
        for i,fn in enumerate(args[:2]):
            p = parse_metadata(load_hex(fn))
            outp = f'src/metadata_packet{i+1}_map.txt'
            Path(outp).write_text('\n'.join(p))
        print('wrote up to two map files in src/')
