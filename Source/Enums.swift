//
//  Enums.swift
//  CocoaMQTT
//
//  Created by Linda Zhong on 27/09/2017.
//  Copyright © 2017 emqtt.io. All rights reserved.
//

import Cocoa

public enum QoS: UInt8 {
    case qos0 = 0
    case qos1 = 1
    case qos2 = 2
}

public enum ConnectAck: UInt8 {
    case accept  = 0
    case unacceptableProtocolVersion
    case identifierRejected
    case serverUnavailable
    case badUsernameOrPassword
    case notAuthorized
    case reserved
}

enum PacketType: UInt8 {
    case reserved = 0x00
    case connect = 0x10
    case connack = 0x20
    case publish = 0x30
    case puback = 0x40
    case pubrec = 0x50
    case pubrel = 0x60
    case pubcomp = 0x70
    case subscribe = 0x80
    case suback = 0x90
    case unsubscribe = 0xA0
    case unsuback = 0xB0
    case pingreq = 0xC0
    case pingresp = 0xD0
    case disconnect = 0xE0
}

enum PresenceType: UInt8 {
    case away = 0
    case online = 1
}
