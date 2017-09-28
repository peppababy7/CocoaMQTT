//
//  Reader.swift
//  CocoaMQTT
//
//  Created by Linda Zhong on 28/09/2017.
//  Copyright © 2017 emqtt.io. All rights reserved.
//

import Cocoa

protocol ReadDelegate {
    func reader(_ reader: Reader, shouldReadToLength length: UInt, tag: Int)
    
    func reader(_ reader: Reader, didReceiveConnAck connAck: ConnAck)
    func reader(_ reader: Reader, didReceivePublish message: String, msgId: UInt16)
    func reader(_ reader: Reader, didReceivePubAck msgId: UInt16)
    func reader(_ reader: Reader, didReceivePubRec msgId: UInt16)
    func reader(_ reader: Reader, didReceivePubRel msgId: UInt16)
    func reader(_ reader: Reader, didReceivePubComp msgId: UInt16)
    func reader(_ reader: Reader, didReceiveSubAck msgId: UInt16)
    func reader(_ reader: Reader, didReceiveUnsubAck msgId: UInt16)
    
    func didReceivePong(_ reader: Reader)
}

// MARK: - public methods

extension Reader {
    
    func start() {
        _buffer = Buffer()
        delegate?.reader(self, shouldReadToLength: 1, tag: ReadTag.header.rawValue)
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
            
        case .payload:
            nextTag = _buffer.fetchPayload(from: data)
        }
        
        let length: UInt
        switch nextTag {
        case .header: length = 1
        case .length: length = 1
        case .payload: length = _buffer.length
        }
        
        delegate?reader(self, shouldReadToLength: length, tag: nextTag)
    }
}

// MARK: -

class Reader: NSObject {
    
    weak var delegate: ReadDelegate?
    private var _buffer = Buffer()
}

// MARK: - private

private extension Reader {
    
    enum ReadTag: Int {
        case header = 0
        case length
        case payload
    }
    
    class Buffer {
        private(set) var header = UInt8(0)
        private(set) var length = UInt(0)
        private(set) var payload = [UInt8]()
        private(set) var multiply = UInt(1)
        
        private(set) var frameReady: Bool = false
        
        /**
         return: read tag next time
         */
        func fetchHeader(from data: Data) -> ReadTag {
            frameReady = false
            length = 0
            payload = []
            multiply = 1
            
            var bytes = [UInt8]()
            data.copyBytes(to: &bytes, count: 1)
            header = bytes[0]
        }
        
        /**
         return: read tag next time
         */
        func fetchLength(from data: Data) -> ReadTag {
            frameReady = false
            
            var bytes = [UInt8]()
            data.copyBytes(to: &bytes, count: 1)
            
            let byte = bytes[0]
            length += (UInt)((Int)(byte & 127) * multiply)
            
            if byte & 0x80 == 0 // read lenght finished
            {
                if length == 0
                {
                    frameReady = true
                    return .header
                } else
                {
                    return .payload
                }
            } else
            {
                multiply *= 128
                return .length
            }
        }
        
        func fetchPayload(from data: Data) -> ReadTag {
            // TODO: Linda -
            return .header
        }
    }
}


private extension Reader {
    func _frameReady() {
        
    }
}
