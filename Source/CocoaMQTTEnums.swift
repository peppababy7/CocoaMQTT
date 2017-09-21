//
//  CocoaMQTTEnums.swift
//  CocoaMQTT
//
//  Created by Linda Zhong on 21/09/2017.
//  Copyright © 2017 emqtt.io. All rights reserved.
//

import Foundation

/**
 * QOS
 */
public enum CocoaMQTTQOS: UInt8 {
    case qos0 = 0
    case qos1
    case qos2
}

/**
 * Connection State
 */
public enum CocoaMQTTConnState: UInt8 {
    case initial = 0
    case connecting
    case connected
    case disconnected
}

/**
 * Conn Ack
 */
public enum CocoaMQTTConnAck: UInt8 {
    case accept  = 0
    case unacceptableProtocolVersion
    case identifierRejected
    case serverUnavailable
    case badUsernameOrPassword
    case notAuthorized
    case reserved
}

/**
 * self presence status
 */
public enum CocoaMQTTPresenceType: UInt8 {
    case away = 0
    case online = 1
}
