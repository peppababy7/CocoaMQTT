//
//  Utils.swift
//  CocoaMQTT
//
//  Created by Linda Zhong on 2019/3/15.
//  Copyright © 2019 emqtt.io. All rights reserved.
//

import Foundation

/// Encode and Decode big-endian UInt16
extension UInt16 {
    var hlBytes: [UInt8] { return [highByte, lowByte] }
    
    // Most Significant Byte (MSB)
    private var highByte: UInt8 { return UInt8( (self & 0xFF00) >> 8) }
    
    // Least Significant Byte (LSB)
    private var lowByte: UInt8 { return UInt8(self & 0x00FF) }
}

/// String with two bytes length
extension String {
    var bytesWithLength: [UInt8] { return UInt16(utf8.count).hlBytes + utf8 }
}

/// Bool to bit
extension Bool {
    var bit: UInt8 { return self ? 1 : 0 }
    
    init(bit: UInt8) { self = (bit == 0) ? false : true }
}

/// read bit
extension UInt8 {
    func bitAt(_ offset: UInt8) -> UInt8 { return (self >> offset) & 0x01 }
}
