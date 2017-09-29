//
//  Frame.swift
//  CocoaMQTT
//
//  Created by Linda Zhong on 27/09/2017.
//  Copyright © 2017 emqtt.io. All rights reserved.
//

import Cocoa
    
// MARK: -
/**
 * |--------------------------------------
 * | 7 6 5 4 |     3    |  2 1  | 0      |
 * |  Type   | DUP flag |  QoS  | RETAIN |
 * |--------------------------------------
 */
final class FixedHeader: EncodeProtocol {
    
    func encode() -> [UInt8] {
        var data = UInt8(0)
        data |= packetType.rawValue
        data |= dup.bit << 3
        data |= qos.rawValue << 1
        data |= retained.bit
        return [data]
    }
    
    init(
        type: PacketType,
        dup: Bool,
        qos: QoS,
        retained: Bool
        ) {
        self.packetType = type
        self.dup = dup
        self.qos = qos
        self.retained = retained
    }
    
    init?(rawValue: UInt8) {
        guard let packetType = PacketType(rawValue: rawValue & 0xF0),
            let qos = QoS(rawValue: (rawValue & 0x06) >> 1) else {
            return nil
        }
        
        self.packetType = packetType
        self.dup = ((rawValue & 0x08) >> 3) == 0
        self.qos = qos
        self.retained = (rawValue & 0x01) == 0
    }
    
    let packetType: PacketType
    let dup: Bool
    let qos: QoS
    let retained: Bool
}

// MARK: -
/**
 * |----------------------------------------------------------------------------------
 * |     7    |    6     |      5     |  4   3  |     2    |       1      |     0    |
 * | username | password | willretain | willqos | willflag | cleansession | reserved |
 * |----------------------------------------------------------------------------------
 */
final class ConnectFlag: EncodeProtocol {
    
    func encode() -> [UInt8] {
        var flags: UInt8 = 0
        flags |= username.bit << 7
        flags |= password.bit << 6
        flags |= willRetain.bit << 5
        flags |= willQoS.rawValue << 3
        flags |= will.bit << 2
        flags |= cleanSession.bit << 1
        return [flags]
    }
    
    var username = false
    var password = false
    var willRetain = false
    var willQoS = QoS.qos0
    var will = false
    var cleanSession = false
}

// MARK: -

final class ConnectPacket: Encoder {
    
    init(
        fixedHeader: FixedHeader,
        clientId: String,
        willMsg: (topic: String, message: String, qos: QoS, retained: Bool)?,
        username: String?,
        password: String?,
        cleanSession: Bool,
        keepAlive: UInt16
        ) {
        self.fixedHeader = fixedHeader

        self.clientId = clientId
        self.willMsg = willMsg
        self.username = username
        self.password = password
        self.cleanSession = cleanSession
        self.keepAlive = keepAlive
    }
    
    let protocolLevel = UInt8(4)
    let protocolVersion = "MQTT/3.1.1"
    let protocolMagic = "MQTT"
    
    let fixedHeader: FixedHeader
    var variableHeader: [UInt8]? {
        var data = [UInt8]()
        
        // version
        data += protocolMagic.bytesWithLength
        data.append(protocolLevel)
        
        // flags
        let flags = ConnectFlag()
        if let _ = username { flags.username = true }
        if let _ = password { flags.password = true }
        if let will = willMsg {
            flags.willRetain = will.retained
            flags.willQoS = will.qos
            flags.will = true
        } else {
            flags.willRetain = false
            flags.willQoS = .qos0
            flags.will = false
        }
        flags.cleanSession = cleanSession
        data += flags.encode()
        
        // keep alive
        data += keepAlive.hlBytes
        
        return data
    }
    var payload: [UInt8]? {
        var data = [UInt8]()
        
        // clientid
        data += clientId.bytesWithLength
        
        // will
        if let will = willMsg {
            data += will.topic.bytesWithLength
            data += will.message.bytesWithLength
        }
        
        // username, password
        if let username = username { data += username.bytesWithLength }
        if let password = password { data += password.bytesWithLength }
        
        return data
    }
    
    private let clientId: String
    private let willMsg: (topic: String, message: String, qos: QoS,  retained: Bool)?
    private let username: String?
    private let password: String?
    private let cleanSession: Bool
    private let keepAlive: UInt16 // seconds
}

final class ConnAckPacket: DecodeProtocol {
    
    static func decode(head: UInt8, data: [UInt8]) -> ConnAckPacket? {
        guard let fixedheader = FixedHeader(rawValue: head),
            fixedheader.packetType == .connack else {
            printError("FixedHeader error: \(head)")
            return nil
        }
        
        guard data.count == 2, let connack = ConnAck(rawValue: data[1]) else {
            printError("ConnAck decode error: \(data[1])")
            return nil
        }
        
        return ConnAckPacket(fixedhead: fixedheader, connack: connack)
    }
    
    init(fixedhead: FixedHeader, connack: ConnAck) {
        self.fixedHeader = fixedhead
        self.connAck = connack
    }
    
    let fixedHeader: FixedHeader
    let connAck: ConnAck
}

final class PubSubAckPacket: DecodeProtocol {
    
    static func decode(head: UInt8, data: [UInt8]) -> PubSubAckPacket? {
        guard let fixedheader = FixedHeader(rawValue: head),
            fixedheader.packetType == .puback,
            fixedheader.packetType == .pubrec,
            fixedheader.packetType == .pubrel,
            fixedheader.packetType == .pubcomp,
            fixedheader.packetType == .suback,
            fixedheader.packetType == .unsuback else {
                printError("FixedHeader error: \(head)")
                return nil
        }
        
        guard data.count >= 2 else {
            printError("data count error: \(data.count)")
            return nil
        }
        
        let msgid = UInt16(data[0]) << 8 + UInt16(data[1])
        
        return PubSubAckPacket(fixedheader: fixedheader, msgid: msgid)
    }
    
    init(fixedheader: FixedHeader, msgid: UInt16) {
        self.fixedHeader = fixedheader
        self.msgid = msgid
    }
    
    let fixedHeader: FixedHeader
    let msgid: UInt16
}

// MARK: -

final class PingPacket: Encoder {
    
    init(presence: PresenceType) {
        self.payload = [presence.rawValue]
    }
    
    let fixedHeader: FixedHeader = FixedHeader(type: .pingreq, dup: false, qos: .qos0, retained: false)
    let variableHeader: [UInt8]? = []
    let payload: [UInt8]?
}

// MARK: -

final class PublishPacket: Encoder, DecodeProtocol {
    
    static func decode(head: UInt8, data: [UInt8]) -> PublishPacket? {
        // fixed header
        guard let fixedhead = FixedHeader(rawValue: head),
            case .publish = fixedhead.packetType else {
            return nil
        }
        
        guard data.count >= 2 else { return nil }
        
        var msb = data[0]
        var lsb = data[1]
        let len = UInt16(msb) << 8 + UInt16(lsb)
        var pos = 2 + Int(len)
        
        // topic
        guard data.count >= pos, let topic = NSString(bytes: [UInt8](data[2...(pos-1)]), length: Int(len), encoding: String.Encoding.utf8.rawValue) as String? else { return nil }
        
        // msgid
        let msgid: UInt16
        if fixedhead.qos == .qos0 {
            msgid = 0
        } else {
            guard data.count >= pos + 2 else { return nil }
            
            msb = data[pos]
            lsb = data[pos + 1]
            pos += 2
            msgid = UInt16(msb) << 8 + UInt16(lsb)
        }
        
        // payload
        let end = data.count - 1
        let payload: [UInt8]
        if end > pos {
            payload = [UInt8](data[pos...end])
        } else {
            payload = []
        }
        
        return PublishPacket(
            fixedhead: fixedhead,
            msgid: msgid,
            topic: topic,
            payload: payload
        )
    }
    
    init?(
        fixedhead: FixedHeader,
        msgid: UInt16,
        topic: String,
        payload: [UInt8]
        ) {
        guard fixedhead.packetType == .publish else { return nil }
        
        self.fixedHeader = fixedhead
        self.msgid = msgid
        self.topic = topic
        
        switch fixedHeader.qos {
        case .qos0: self.variableHeader = topic.bytesWithLength
        case .qos1, .qos2: self.variableHeader = topic.bytesWithLength + msgid.hlBytes
        }
        
        self.payload = payload
    }
    
    let fixedHeader: FixedHeader
    let variableHeader: [UInt8]?
    let payload: [UInt8]?
    
    let topic: String
    let msgid: UInt16
}

// MARK: -

final class PubAckPacket: DecodeProtocol {
 
    static func decode(head: UInt8, data: [UInt8]) -> PubAckPacket? {
        guard data.count >= 2, let fixedhead = FixedHeader(rawValue: head) else { return nil }
        
        let msb = data[0]
        let lsb = data[1]
        let msgid = UInt16(msb) << 8 + UInt16(lsb)
        
        return PubAckPacket(fixedhead: fixedhead, msgid: msgid)
    }
    
    init(fixedhead: FixedHeader, msgid: UInt16) {
        self.fixedHeader = fixedhead
        self.msgid = msgid
    }
    
    let fixedHeader: FixedHeader
    let msgid: UInt16
}

// MARK: -

final class SubscribePacket: Encoder {
    init?(
        fixedHeader: FixedHeader,
        msgid: UInt16,
        topic: String,
        reqos: QoS
        ) {
        guard fixedHeader.packetType == .subscribe else { return nil }
        
        self.fixedHeader = fixedHeader
        self.msgid = msgid
        self.variableHeader = msgid.hlBytes
        self.topic = topic
        self.reqos = reqos
        var result = topic.bytesWithLength
        result.append(reqos.rawValue)
        self.payload = result
    }
    
    let fixedHeader: FixedHeader
    let variableHeader: [UInt8]?
    let payload: [UInt8]?
    
    let msgid: UInt16
    let topic: String
    let reqos: QoS
}

// MARK: -

final class UnsubscribePacket: Encoder {
    
    init?(
        fixedHeader: FixedHeader,
        msgid: UInt16,
        topic: String
        ) {
        guard fixedHeader.packetType == .unsubscribe else { return nil }
        
        self.fixedHeader = fixedHeader
        self.msgid = msgid
        self.variableHeader = msgid.hlBytes
        self.topic = topic
        self.payload = topic.bytesWithLength
    }
    
    let fixedHeader: FixedHeader
    let variableHeader: [UInt8]?
    let payload: [UInt8]?
    
    let msgid: UInt16
    let topic: String
}


// MARK: - protocols

protocol EncodeProtocol {
    func encode() -> [UInt8]
}

protocol DecodeProtocol {
    static func decode(head: UInt8, data: [UInt8]) -> Self?
}

protocol PacketProtocol: EncodeProtocol {
    var fixedHeader: FixedHeader { get }
    var variableHeader: [UInt8]? { get }
    var payload: [UInt8]? { get }
}

protocol Encoder: PacketProtocol, EncodeProtocol { }

extension Encoder {
    func encode() -> [UInt8] {
        var buffer = fixedHeader.encode()
        
        let variable = variableHeader ?? []
        let payloads = payload ?? []
        
        let length = UInt32(variable.count) + UInt32(payloads.count)
        
        buffer += length.encode()
        buffer += variable
        buffer += payloads
        
        return buffer
    }
}

extension UInt32: EncodeProtocol {
    func encode() -> [UInt8] {
        var bytes = [UInt8]()
        var digit: UInt8 = 0
        var len = self
        
        repeat {
            digit = UInt8(len & 0x7F)
            len = len >> 7
            if len > 0 { digit = digit | 0x80 }
            
            bytes.append(digit)
        } while len > 0
        
        return bytes
    }
}
