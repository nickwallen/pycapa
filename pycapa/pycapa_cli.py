#!/usr/local/bin

import pcapy
import argparse
import kafka
import struct
import random
from datetime import datetime

MICROS_PER_SEC = 1000000.0

def make_parser():

    parser = argparse.ArgumentParser()
    parser.add_argument('-p', '--producer', help='sniff packets, send to kafka', dest='producer', action='store_true', default=True)
    parser.add_argument('-c', '--consumer', help='consume packets from kafka', dest='consumer', action='store_true', default=False)
    parser.add_argument('-k', '--kafka', help='kafka broker(s), comma-separated', dest='kafka_brokers')
    parser.add_argument('-t', '--topic', help='kafka topic destination', dest='topic')
    parser.add_argument('-l', '--local', help='print packet instead of sending to kafka', dest='local', action='store_true', default=False)
    parser.add_argument('-d', '--debug', help='enable debug messages', dest='debug', action='store_true', default=False)
    parser.add_argument('-i', '--interface', help='interface to listen on', dest='interface')
    return parser

def valid_args(args):

    # as a producer, we need the kafka broker, topic and an interface
    if args.producer and args.kafka_brokers and args.topic and args.interface:
        return True
    # as a consumer, we need the kafka broker and a topic
    elif args.consumer and args.kafka_brokers and args.topic:
        return True
    else:
        return False

def formatted(s):

    hex_string =  ' '.join("{0:02x}".format(ord(c)) for c in s)
    return  '\n'.join([ hex_string[i:i+48] for i in range(0, len(hex_string), 48) ])

def as_date(epoch_micros):

    epoch_secs = epoch_micros / MICROS_PER_SEC
    return datetime.fromtimestamp(epoch_secs).strftime('%Y-%m-%d %H:%M:%S.%f')

def partitioner(key_bytes, all_partitions, available_partitions):

    return random.choice(available_partitions)

def producer(args):

    # connect to kafka
    brokers = args.kafka_brokers.split(",")
    producer = kafka.KafkaProducer(
        bootstrap_servers=brokers,
        partitioner=partitioner)

    # init packet capture
    capture = pcapy.open_live(args.interface, 65535, True, 3000)
    packet_count = 0

    # start packet capture
    while True:
        (pkt_hdr, pkt_raw) = capture.next()
        if pkt_hdr is not None:
            packet_count += 1

            # create timestamp in epoch microseconds
            (epoch_secs, delta_micros) = pkt_hdr.getts()
            epoch_micros = (epoch_secs * MICROS_PER_SEC) + delta_micros

            # message key is network-order 64-bit unsigned
            msg_key = struct.pack(">Q", epoch_micros)

            # send packet to kafka
            producer.send(args.topic, key=msg_key, value=pkt_raw)
            if args.debug:
                print 'Sent Packet: ts=%s topic=%s count=%s' % (
                    as_date(epoch_micros), args.topic, packet_count)
                print formatted(pkt_raw)
            elif packet_count % 100 == 0:
                print 'Sent %s packets' % packet_count

def consumer(args):

    # connect to kafka
    brokers = args.kafka_brokers.split(",")
    kconsumer = kafka.KafkaConsumer(args.topic, bootstrap_servers=brokers)

    # start packet capture
    packet_count = 0
    for msg in kconsumer:
        packet_count += 1

        # packet timestamp
        epoch_micros = struct.unpack_from(">Q", bytes(msg.key), 0)[0]

        print 'Received Packet: ts=%s topic=%s partition=%s offset=%s count=%s' % (
            as_date(epoch_micros), msg.topic, msg.partition, msg.offset, packet_count)
        print formatted(msg.value)

def main():

    # parse command-line args
    parser = make_parser()
    args = parser.parse_args()

    if not valid_args(args):
        parser.print_help()
    elif args.consumer:
        consumer(args)
    elif args.producer:
        producer(args)
    else:
        parser.print_help()

if __name__ == '__main__':
  main()
