//
//  Utils.swift
//  CocoaMQTT
//
//  Created by Linda Zhong on 27/09/2017.
//  Copyright © 2017 emqtt.io. All rights reserved.
//

import Cocoa

extension Bool {
    var bit: UInt8 { return self ? 1 : 0 }
}

// MARK: - decode mqtt length byte

extension UInt8 {
    var digit: UInt8 { return self & 127 }
    
    var finish: Bool { return (self & 0x80) == 0 }
}

extension UInt16 {
    // Most Significant Byte (MSB)
    var highByte: UInt8 { return UInt8( (self & 0xFF00) >> 8) }
    
    // Least Significant Byte (LSB)
    var lowByte: UInt8 { return UInt8(self & 0x00FF) }
    
    var hlBytes: [UInt8] { return [highByte, lowByte] }
}


extension String {
    var bytesWithLength: [UInt8] { return UInt16(utf8.count).hlBytes + utf8 }
}
