//
//  CocoaMQTT+Rd.swift
//  CocoaMQTT
//
//  Created by Linda Zhong on 2019/3/15.
//  Copyright © 2019 emqtt.io. All rights reserved.
//

import Foundation

import CocoaAsyncSocket

protocol ReaderDelegate: class {
    func reader(_ reader: CocoaMQTT.Reader, didReceiveConnAck connack: UInt8)
    func reader(_ reader: CocoaMQTT.Reader, didReceivePublish message: CocoaMQTT.Message, id: UInt16)
    func reader(_ reader: CocoaMQTT.Reader, didReceivePubAck msgid: UInt16)
    func reader(_ reader: CocoaMQTT.Reader, didReceivePubRec msgid: UInt16)
    func reader(_ reader: CocoaMQTT.Reader, didReceivePubRel msgid: UInt16)
    func reader(_ reader: CocoaMQTT.Reader, didReceivePubComp msgid: UInt16)
    func reader(_ reader: CocoaMQTT.Reader, didReceiveSubAck msgid: UInt16)
    func reader(_ reader: CocoaMQTT.Reader, didReceiveUnsubAck msgid: UInt16)
    func didReceivePong(_ reader: CocoaMQTT.Reader)
}


extension CocoaMQTT {

    /**
        read header length  -->  read header  -->  read body length  -->  read body
     */
    final class Reader {
        enum Tag: Int { case header = 0, length, payload }

        init(socket: GCDAsyncSocket, delegate: ReaderDelegate) {
            self.socket = socket
            self.delegate = delegate
        }
        
        func start() { _readHeader() }
        
        func headerReady(_ header: UInt8) {
            printDebug("reader header ready: \(header) ")
            
            self.header = header
            _readLength()
        }
        
        func lengthReady(_ byte: UInt8) {
            length += (UInt)((Int)(byte & 127) * multiply)
            // done
            if byte & 0x80 == 0 {
                if length == 0 {
                    _frameReady()
                } else {
                    _readPayload()
                }
                // more
            } else {
                multiply *= 128
                _readLength()
            }
        }
        
        func payloadReady(_ data: Data) {
            self.data = [UInt8](repeating: 0, count: data.count)
            data.copyBytes(to: &(self.data), count: data.count)
            _frameReady()
        }
        
        fileprivate let socket: GCDAsyncSocket
        fileprivate var header: UInt8 = 0
        fileprivate var length: UInt = 0
        fileprivate var data: [UInt8] = []
        fileprivate var multiply = 1
        fileprivate var timeout = 30000
        fileprivate weak var delegate: ReaderDelegate?
    }
}

private extension CocoaMQTT.Reader {
    func _readHeader() {
        length = 0
        multiply = 1
        header = 0
        data = []
        socket.readData(toLength: 1, withTimeout: -1, tag: Tag.header.rawValue)
    }
    
    func _readLength() {
        socket.readData(toLength: 1, withTimeout: TimeInterval(timeout), tag: Tag.length.rawValue)
    }
    
    func _readPayload() {
        socket.readData(toLength: length, withTimeout: TimeInterval(timeout), tag: Tag.payload.rawValue)
    }
    
    func _frameReady() {
        // handle frame
        let frameType = CocoaMQTT.Frame.TYPE(rawValue: UInt8(header & 0xF0))!
        switch frameType {
        case .connack:
            delegate?.reader(self, didReceiveConnAck: data[1])
        case .publish:
            if let tuple = _unpackPublish() { delegate?.reader(self, didReceivePublish: tuple.msg, id: tuple.msgId) }
        case .puback:
            delegate?.reader(self, didReceivePubAck: _msgid(data))
        case .pubrec:
            delegate?.reader(self, didReceivePubRec: _msgid(data))
        case .pubrel:
            delegate?.reader(self, didReceivePubRel: _msgid(data))
        case .pubcomp:
            delegate?.reader(self, didReceivePubComp: _msgid(data))
        case .suback:
            delegate?.reader(self, didReceiveSubAck: _msgid(data))
        case .unsuback:
            delegate?.reader(self, didReceiveUnsubAck: _msgid(data))
        case .pingresp:
            delegate?.didReceivePong(self)
        default:
            break
        }
        
        _readHeader()
    }
    
    func _unpackPublish() -> (msgId: UInt16, msg: CocoaMQTT.Message)? {
        let frame = CocoaMQTT.PublishFrame(header: header, data: data)
        frame.unpack()
        guard let msgid = frame.msgid, let qos = CocoaMQTT.QOS(rawValue: frame.qos), let topic = frame.topic else { return nil }
        
        return (
            msgId: msgid,
            msg: CocoaMQTT.Message(
                topic: topic,
                payload: frame.payload,
                qos: qos,
                retained: frame.retained,
                dup: frame.dup
            )
        )
    }
    
    func _msgid(_ bytes: [UInt8]) -> UInt16 {
        if bytes.count < 2 { return 0 }
        return UInt16(bytes[0]) << 8 + UInt16(bytes[1])
    }
}
