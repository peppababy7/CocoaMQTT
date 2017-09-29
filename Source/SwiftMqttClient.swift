//
//  SwiftMqttClient.Swift
//  CocoaMQTT
//
//  Created by Linda Zhong on 28/09/2017.
//  Copyright © 2017 emqtt.io. All rights reserved.
//

import Cocoa
import SwiftyTimer
import CocoaAsyncSocket

// MARK: -

protocol SwiftMqttClientDelegate: NSObjectProtocol {
    func mqtt(_ mqtt: SwiftMqttClient, didConnectToHost host: String, port: UInt16)
    func mqtt(_ mqtt: SwiftMqttClient, didConnectWithError error: MqttClientError)
    func mqtt(_ mqtt: SwiftMqttClient, didDisconnectWithError error: MqttClientError?)
    
    func mqtt(_ mqtt: SwiftMqttClient, didSubscribeTopic topic: String)
    func mqtt(_ mqtt: SwiftMqttClient, didSubscribeWithError error: MqttClientError)
    func mqtt(_ mqtt: SwiftMqttClient, didUnsubscribeTopic topic: String)
    func mqtt(_ mqtt: SwiftMqttClient, didUnsubscribeWithError error: MqttClientError)
    
    func mqtt(_ mqtt: SwiftMqttClient, didPublishTopic topic: String)
    func mqtt(_ mqtt: SwiftMqttClient, didPublishWithError error: MqttClientError)
}

// MARK: - public methods

extension SwiftMqttClient {
    
    func connect() {
        _initialize()
        
        do {
            try _socket.connect(toHost: host, onPort: port)
            state = .connecting
        } catch {
            state = .disconnected
            _delegate?.mqtt(self, didConnectWithError: error)
        }
    }
    
    func disconnect() {
        let bytes = FixedHeader(type: .disconnect, dup: false,
                                qos: .qos0, retained: false).encode()
        _socket.write(Data(bytes: bytes, count: bytes.count), withTimeout: -1, tag: 0)
        
        _socket.disconnect()
    }
    
    func publish(
        topic: String,
        message: String,
        qos: QoS = .qos1,
        retained: Bool = false,
        dup: Bool = false
        ) {
        guard case .connected = state else {
            _delegate?.mqtt(self, didPublishWithError: .stateError)
            return
        }
        
        guard let packet = PublishPacket(
            fixedhead: FixedHeader(type: .publish, dup: dup,
                                   qos: qos, retained: retained),
            msgid: _msgid,
            topic: topic,
            payload: [UInt8](message.utf8)
            ) else {
            _delegate?.mqtt(self, didPublishWithError: .paraError)
            return
        }
        
        let data = packet.encode()
        _socket.write(Data(bytes: data, count: data.count), withTimeout: -1, tag: 0)
    }
    
    func subscribe(
        topic: String,
        qos: QoS = .qos1
        ) {
        guard case .connected = state else {
            _delegate?.mqtt(self, didSubscribeWithError: .stateError)
            return
        }
        
        guard let packet = SubscribePacket(
            fixedHeader: FixedHeader(type: .subscribe, dup: false,
                                     qos: qos, retained: false),
            msgid: _msgid,
            topic: topic,
            reqos: qos) else {
            _delegate?.mqtt(self, didSubscribeWithError: .paraError)
            return
        }
        
        let data = packet.encode()
        _socket.write(Data(bytes: data, count: data.count), withTimeout: -1, tag: 0)
    }
    
    func unsubscribe(topic: String) {
        guard case .connected = state else {
            _delegate?.mqtt(self, didUnsubscribeWithError: .stateError)
            return
        }
        
        guard let packet = UnsubscribePacket(
            fixedHeader: FixedHeader(type: .unsubscribe, dup: false,
                                     qos: .qos0, retained: false),
            msgid: _msgid,
            topic: topic) else {
            _delegate?.mqtt(self, didUnsubscribeWithError: .paraError)
            return
        }
        
        let data = packet.encode()
        _socket.write(Data(bytes: data, count: data.count), withTimeout: -1, tag: 0)
    }
}

// MARK: - config

final class SwiftMqttClient: NSObject {
    
    enum State {
        case initial
        case connecting
        case connected
        case disconnected
    }
    
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
        keepAlive: TimeInterval,
        cleanSession: Bool,
        enableSSL: Bool,
        allowUntrustCA: Bool
        ) {
        self.username = username
        self.password = password
        self.keepAlive = keepAlive
        self.cleanSession = cleanSession
        self.enableSSL = enableSSL
        self.allowUntrustCA = allowUntrustCA
    }
    
    func setDelegate(
        _ delegate: SwiftMqttClientDelegate,
        onQueue queue: DispatchQueue
        ) {
        _socket.setDelegate(self, delegateQueue: queue)
        _queue = queue
        _delegate = delegate
    }
    
    // MARK: -- properties
    
    let clientId: String
    let host: String
    let port: UInt16
    
    var presence = PresenceType.online {
        willSet {
            guard newValue != presence else { return }
            
            _pingPong.start()
        }
    }
    
    fileprivate(set) var username: String? = nil
    fileprivate(set) var password: String? = nil
    fileprivate(set) var keepAlive: TimeInterval = 60
    fileprivate(set) var cleanSession: Bool = false
    fileprivate(set) var enableSSL: Bool = true
    fileprivate(set) var allowUntrustCA: Bool = false
    
    fileprivate(set) var state: State = .initial
    
    fileprivate var _socket = GCDAsyncSocket()
    fileprivate var _reader = Reader()
    fileprivate var _pingPong = PingPong(keepAlive: keepAlive, delegate: self as! PingPongDelegate)
    fileprivate var _queue = DispatchQueue.main
    fileprivate weak var _delegate: SwiftMqttClientDelegate?
    
    fileprivate var _msgid: UInt16 = 123 // TODO: Linda - should plus
    
    fileprivate var _
}

// MARK: - GCDAsyncSocketDelegate

extension SwiftMqttClient: GCDAsyncSocketDelegate {
    
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

extension SwiftMqttClient: PingPongDelegate {
    func ping() {
        let data = PingPacket(presence: presence).encode()
        _socket.write(Data(bytes: data, count: data.count), withTimeout: -1, tag: -0xC0)
    }
    
    func pongDidTimeOut() {
        
    }
}

// MARK: - ReadDelegate

extension SwiftMqttClient: ReadDelegate {
    
    func reader(_ reader: Reader, shouldReadLength length: UInt, tag: Int) {
        _socket.readData(toLength: length, withTimeout: -1, tag: tag) // TODO: Linda - need fix
    }
    
    func reader(_ reader: Reader, didReceiveConnAck connAck: ConnAck) {
        switch connAck {
        case .accept:
            state = .connected
            _delegate?.mqtt(self, didConnectToHost: host, port: port)
            return
            
        default:
            state = .disconnected
            _delegate?.mqtt(self, didConnectWithError: .connAckError)
            disconnect()
            return
        }
    }
    
    func reader(_ reader: Reader, didReceivePublish packet: PublishPacket) {
        _delegate?.mqtt(self, didReceivePublish: packet)
    }
    
    func reader(_ reader: Reader, didReceivePubSubAck type: PacketType, msgId: UInt16) {
        switch type {
        case .suback: // TODO: Linda -
            _delegate?.mqtt(<#T##mqtt: SwiftMqttClient##SwiftMqttClient#>, didSubscribeTopic: <#T##String#>)
        default: return // TODO: Linda - handle publish
        }
    }
    
    func didReceivePong(_ reader: Reader){
        
    }
}

// MARK: - private

private extension SwiftMqttClient {
    
    func _initialize() {
        _socket.delegate = nil
        _socket = GCDAsyncSocket()
        _socket.setDelegate(self, delegateQueue: _queue)
        
        state = .initial
        
        _reader.delegate = nil
        _reader = Reader()
        _reader.delegate = self
        
        _pingPong = PingPong(keepAlive: keepAlive, delegate: self as! PingPongDelegate)
    }
    
    func _handleSocketConnect() {
        
        _reader.start()
    }
}

