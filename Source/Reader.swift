//
//  Reader.swift
//  CocoaMQTT
//
//  Created by Linda Zhong on 28/09/2017.
//  Copyright © 2017 emqtt.io. All rights reserved.
//

import Cocoa

protocol ReadDelegate: NSObjectProtocol {
    func reader(_ reader: Reader, shouldReadLength length: UInt, tag: Int)
    
    func reader(_ reader: Reader, didReceiveConnAck conAck: ConnAck)
    func reader(_ reader: Reader, didReceivePublish packet: PublishPacket)
    
    func reader(_ reader: Reader, didReceivePubSubAck type: PacketType, msgId: UInt16)
    
    func didReceivePong(_ reader: Reader)
}

// MARK: - public methods

extension Reader {
    
    func start() {
        _buffer = Buffer()
        delegate?.reader(
            self,
            shouldReadLength: 1,
            tag: ReadTag.header.rawValue
        )
    }
    
    func fetchData(tag: Int, from data: Data) {
        guard let readTag = ReadTag(rawValue: tag) else { return }
        
        let nextTag: ReadTag
        switch readTag {
        case .header:
            if _buffer.frameReady == true { _frameReady() }
            
            _buffer = Buffer()
            nextTag = _buffer.fetchHeader(from: data)
            
        case .length:
            nextTag = _buffer.fetchLength(from: data)
            
        case .data:
            nextTag = _buffer.fetchPayload(from: data)
        }
        
        let length: UInt
        switch nextTag {
        case .header: length = 1
        case .length: length = 1
        case .data: length = _buffer.length
        }
        
        delegate?.reader(
            self,
            shouldReadLength: length,
            tag: nextTag.rawValue
        )
    }
}

// MARK: -

class Reader: NSObject {
        
    // MARK: -- properties
    
    weak var delegate: ReadDelegate?
    fileprivate var _buffer = Buffer()
}

// MARK: - delegate packet out

private extension Reader {
    
    func _frameReady() {
        guard let fixedHead = FixedHeader(rawValue: _buffer.header) else {
            printError("FixedHead type error: \(_buffer.header)")
            return
        }
        
        switch fixedHead.packetType {
        case .connack:
            guard let packet = ConnAckPacket.decode(head: _buffer.header, data: _buffer.data) else { return }

            delegate?.reader(self, didReceiveConnAck: packet.connAck)
            
        case .puback, .pubcomp, .pubrec, .pubrel, .suback, .unsuback:
            guard let packet = PubAckPacket.decode(head: _buffer.header, data: _buffer.data) else { return }
            
            delegate?.reader(
                self,
                didReceivePubSubAck: packet.fixedHeader.packetType,
                msgId: packet.msgid
            )
            
        case .pingresp:
            delegate?.didReceivePong(self)
            
        case .publish:
            guard let packet = PublishPacket.decode(head: _buffer.header,data: _buffer.data) else { return }
            
            delegate?.reader(
                self,
                didReceivePublish: packet
            )
            
        default: return
        }
    }
}

// MARK: - private

private extension Reader {
    
    enum ReadTag: Int {
        case header = 0
        case length
        case data
    }
    
    class Buffer {
        private(set) var header = UInt8(0)
        private(set) var length = UInt(0) // length of bytes
        private(set) var data = [UInt8]()
        private(set) var multiply = UInt(1)
        
        private(set) var frameReady: Bool = false
        
        /**
         return: read tag next time
         */
        func fetchHeader(from data: Data) -> ReadTag {
            frameReady = false
            length = 0
            self.data = []
            multiply = 1
            
            var bytes = [UInt8]()
            data.copyBytes(to: &bytes, count: 1)
            header = bytes[0]
            return .length
        }
        
        /**
         return: read tag next time
         */
        func fetchLength(from data: Data) -> ReadTag {
            frameReady = false
            
            var bytes = [UInt8]()
            data.copyBytes(to: &bytes, count: 1)
            
            let byte = bytes[0]
            length += (UInt)((UInt)(byte.digit) * multiply)
            
            if byte.finish == true
            {
                if length == 0
                {
                    frameReady = true
                    return .header
                } else
                {
                    return .data
                }
            } else
            {
                multiply *= 128
                return .length
            }
        }
        
        /**
         return: read tag next time
         */
        func fetchPayload(from data: Data) -> ReadTag {
            self.data = [UInt8](repeating: 0, count: data.count)
            data.copyBytes(to: &self.data, count: data.count)
            
            return .header
        }
    }
}

