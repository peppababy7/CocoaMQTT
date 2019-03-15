//
//  Enums.swift
//  CocoaMQTT
//
//  Created by Linda Zhong on 2019/3/15.
//  Copyright © 2019 emqtt.io. All rights reserved.
//

import Foundation

public extension CocoaMQTT {
    @objc enum QOS: UInt8 {
        case qos0 = 0
        case qos1
        case qos2
    }
    
    @objc enum ConnState: UInt8 {
        case initial = 0
        case connecting
        case connected
        case disconnected
    }
    
    @objc enum ConnAck: UInt8 {
        case accept  = 0
        case unacceptableProtocolVersion
        case identifierRejected
        case serverUnavailable
        case badUsernameOrPassword
        case notAuthorized
        case reserved
    }
}
