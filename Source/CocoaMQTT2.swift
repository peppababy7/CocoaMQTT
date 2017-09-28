//
//  CocoaMQTT2.swift
//  CocoaMQTT
//
//  Created by Linda Zhong on 28/09/2017.
//  Copyright © 2017 emqtt.io. All rights reserved.
//

import Cocoa
import SwiftyTimer
import CocoaAsyncSocket

// MARK: - public methods

extension CocoaMQTT2 {
    
    func connect() {
        
    }
    
    func disconnect() {
        
    }
    
    func setPresence(_ presence: PresenceType) {
        self.presence = presence
    }
    
    @discardableResult
    func publish(
        topic: String,
        message: String,
        qos: QoS = .qos1,
        retained: Bool = false,
        dup: Bool = false
        ) -> UInt16 {
        
    }
    
    @discardableResult
    func subscribe(
        topic: String,
        qos: QoS = .qos1
        ) -> UInt16 {
        
    }
    
    @discardableResult
    func unsubscribe(topic: String) -> UInt16 {
        
    }
}

// MARK: - config

final class CocoaMQTT2: NSObject {
    
    init(
        clientId: String,
        host: String,
        port: UInt16
        ) {
        self.clientId = clientId
        self.host = host
        self.port = port
    }
    
    func config(
        username: String?,
        password: String?,
        cleanSession: Bool,
        enableSSL: Bool,
        allowUntrustCA: Bool
        ) {
        self.username = username
        self.password = password
        self.cleanSession = cleanSession
        self.enableSSL = enableSSL
        self.allowUntrustCA = allowUntrustCA
    }
    
    func setDelegate(
        _ delegate: DELEGATE,
        onQueue queue: DispatchQueue
        ) {
        _socket.setDelegate(self, delegateQueue: queue)
        _queue = queue
    }
    
    // MARK: -- properties
    
    let clientId: String
    let host: String
    let port: UInt16
    
    fileprivate(set) var enableSSL: Bool = true
    fileprivate(set) var allowUntrustCA: Bool = false
    fileprivate(set) var username: String? = nil
    fileprivate(set) var password: String? = nil
    fileprivate(set) var cleanSession: Bool = false
    fileprivate(set) var enableSSL: Bool = true
    fileprivate(set) var allowUntrustCA: Bool = false
    fileprivate(set) var presence = PresenceType.online
    
    fileprivate var _socket = GCDAsyncSocket()
    fileprivate var _reader = Reader()
    fileprivate var _pingPong = PingPong()
    fileprivate var _queue = DispatchQueue.main
}

// MARK: - GCDAsyncSocketDelegate

extension CocoaMQTT2: GCDAsyncSocketDelegate {
    
    func socket(
        _ sock: GCDAsyncSocket,
        didConnectToHost host: String,
        port: UInt16
        ) {
        if enableSSL == true
        {
            let sslSettings = [
                        GCDAsyncSocketManuallyEvaluateTrust as String :
                        NSNumber(value: allowUntrustCA)
            ]
            _socket.startTLS(sslSettings)
        }
        else
        {
            _handleSocketConnect()
        }
    }
    
    // TODO: Linda - trust
    
    func socketDidSecure(_ sock: GCDAsyncSocket) {
        _handleSocketConnect()
    }
    
    func socket(
        _ sock: GCDAsyncSocket,
        didRead data: Data,
        withTag tag: Int
        ) {
        _reader.fetchData(tag: tag, from: data)
    }
    
    func socket(
        _ sock: GCDAsyncSocket,
        didWriteDataWithTag tag: Int
        ) {
        printDebug("AsyncSock Socket write message with tag: \(tag)")
    }
}

// MARK: - PingPongDelegate

extension CocoaMQTT2: PingPongDelegate {
    func ping() {
        let data = PingPacket(presence: presence).encode()
        _socket.write(Data(bytes: data, count: data.count), withTimeout: -1, tag: -0xC0)
    }
    
    func pongDidTimeOut() {
        
    }
}

// MARK: - ReadDelegate

extension CocoaMQTT2: ReadDelegate {
    
    func reader(_ reader: Reader, shouldReadToLength length: UInt, tag: Int) {
        _socket.readData(toLength: length, withTimeout: -1, tag: tag) // TODO: Linda - need fix
    }
    
    func reader(_ reader: Reader, didReceiveConnAck connAck: ConnectAck) {
        
    }
    
    func reader(_ reader: Reader, didReceivePublish message: String, msgId: UInt16) {
        
    }
    
    func reader(_ reader: Reader, didReceivePubAck msgId: UInt16) {
        
    }
    
    func reader(_ reader: Reader, didReceivePubRec msgId: UInt16) {
        
    }
    
    func reader(_ reader: Reader, didReceivePubRel msgId: UInt16) {
        
    }
    
    func reader(_ reader: Reader, didReceivePubComp msgId: UInt16) {
        
    }
    
    func reader(_ reader: Reader, didReceiveSubAck msgId: UInt16) {
        
    }
    
    func reader(_ reader: Reader, didReceiveUnsubAck msgId: UInt16){
        
    }
    
    func didReceivePong(_ reader: Reader) {
        
    }
}

// MARK: - private

private extension CocoaMQTT2 {
    
    func _handleSocketConnect() {
        
        _reader.start()
    }
    
    
}

protocol DELEGATE {
}
