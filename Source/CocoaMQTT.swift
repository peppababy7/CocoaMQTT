//
//  CocoaMQTT.swift
//  CocoaMQTT
//
//  Created by Feng Lee<feng@eqmtt.io> on 14/8/3.
//  Copyright (c) 2015 emqtt.io. All rights reserved.
//

import Foundation
import CocoaAsyncSocket
import SwiftyTimer


/**
 * QOS
 */
@objc public enum CocoaMQTTQOS: UInt8 {
    case qos0 = 0
    case qos1
    case qos2
}

/**
 * Connection State
 */
@objc public enum CocoaMQTTConnState: UInt8 {
    case initial = 0
    case connecting
    case connected
    case disconnected
}

/**
 * Conn Ack
 */
@objc public enum CocoaMQTTConnAck: UInt8 {
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
@objc public enum CocoaMQTTPresenceType: UInt8 {
    case away = 0
    case online = 1
}

/**
 * asyncsocket read tag
 */
fileprivate enum CocoaMQTTReadTag: Int {
    case header = 0
    case length
    case payload
}

/**
 * MQTT log delegate
 */
@objc public protocol CocoaMqttLogProtocol: NSObjectProtocol {
    var minLevel: CocoaMQTTLoggerLevel { get }
    func log(level: CocoaMQTTLoggerLevel, message: String)
}

/**
 * MQTT Delegate
 */
@objc public protocol CocoaMQTTDelegate: CocoaMqttLogProtocol {
    func mqtt(_ mqtt: CocoaMQTT, didConnect host: String, port: Int)
    func mqtt(_ mqtt: CocoaMQTT, didConnectAck ack: CocoaMQTTConnAck)
    func mqtt(_ mqtt: CocoaMQTT, didPublishMessage message: CocoaMQTTMessage, id: UInt16)
    func mqtt(_ mqtt: CocoaMQTT, didPublishAck id: UInt16)
    func mqtt(_ mqtt: CocoaMQTT, didReceiveMessage message: CocoaMQTTMessage, id: UInt16 )
    func mqtt(_ mqtt: CocoaMQTT, didSubscribeTopic topic: String)
    func mqtt(_ mqtt: CocoaMQTT, didUnsubscribeTopic topic: String)
    func mqttDidPing(_ mqtt: CocoaMQTT)
    func mqttDidReceivePong(_ mqtt: CocoaMQTT)
    func mqttDidDisconnect(_ mqtt: CocoaMQTT, withError err: Error?)
    
    var minLevel: CocoaMQTTLoggerLevel { get }
    func log(level: CocoaMQTTLoggerLevel, message: String)
    
    @objc optional func mqtt(_ mqtt: CocoaMQTT, didReceive trust: SecTrust, completionHandler: @escaping (Bool) -> Void)
    @objc optional func mqtt(_ mqtt: CocoaMQTT, didPublishComplete id: UInt16)
}

/**
 * Blueprint of the MQTT client
 */
internal protocol CocoaMQTTClient {
    var host: String { get set }
    var port: UInt16 { get set }
    var clientID: String { get }
    var username: String? { get set }
    var password: String? { get set }
    var cleanSession: Bool { get set }
    var keepAlive: UInt16 { get set }
    var willMessage: CocoaMQTTWill? { get set }
    var presence: CocoaMQTTPresenceType { get set }
    
    func connect() -> Bool
    func disconnect()
    func ping()
    
    func subscribe(_ topic: String, qos: CocoaMQTTQOS) -> UInt16
    func unsubscribe(_ topic: String) -> UInt16
    func publish(_ topic: String, withString string: String, qos: CocoaMQTTQOS, retained: Bool, dup: Bool) -> UInt16
    func publish(_ message: CocoaMQTTMessage) -> UInt16
}

/**
 * MQTT Reader Delegate
 */
internal protocol CocoaMQTTReaderDelegate {
    func didReceiveConnAck(_ reader: CocoaMQTTReader, connack: UInt8)
    func didReceivePublish(_ reader: CocoaMQTTReader, message: CocoaMQTTMessage, id: UInt16)
    func didReceivePubAck(_ reader: CocoaMQTTReader, msgid: UInt16)
    func didReceivePubRec(_ reader: CocoaMQTTReader, msgid: UInt16)
    func didReceivePubRel(_ reader: CocoaMQTTReader, msgid: UInt16)
    func didReceivePubComp(_ reader: CocoaMQTTReader, msgid: UInt16)
    func didReceiveSubAck(_ reader: CocoaMQTTReader, msgid: UInt16)
    func didReceiveUnsubAck(_ reader: CocoaMQTTReader, msgid: UInt16)
    func didReceivePong(_ reader: CocoaMQTTReader)
}

/**
 * Heart Beats Protocol
 */
internal protocol PingPongProtocol: NSObjectProtocol {
    func ping()
    func pongDidTimeOut()
}

extension Int {
    var MB: Int {
        return self * 1024 * 1024
    }
}

/**
 * Main CocoaMQTT Class
 *
 * Notice: GCDAsyncSocket need delegate to extend NSObject
 */
public class CocoaMQTT: NSObject, CocoaMQTTClient, CocoaMQTTFrameBufferProtocol {
    public var host = "localhost"
    public var port: UInt16 = 1883
    public var clientID: String
    public var username: String?
    public var password: String?
    public var secureMQTT = false
    public var cleanSession = true
    public var willMessage: CocoaMQTTWill?
    public var presence: CocoaMQTTPresenceType = .away
    public weak var delegate: CocoaMQTTDelegate? {
        didSet { CocoaMQTTLogger.shared.delegate = delegate }
    }
    public var backgroundOnSocket = false
    public fileprivate(set) var connState = CocoaMQTTConnState.initial
    public var dispatchQueue = DispatchQueue.main
    
    // flow control
    var buffer = CocoaMQTTFrameBuffer()
    
    // heart beat
    public var keepAlive: UInt16 = 60 {
        didSet { pingpong = PingPong(keepAlive: Double(keepAlive / 2 + 1).seconds, delegate: self) }
    }
    fileprivate var pingpong: PingPong?
    
    // auto reconnect
    public var retrySetting: (retry: Bool, maxCount: UInt, step: TimeInterval, fusingDepth: UInt, fusingDuration: TimeInterval) = (true, 10, 1.2, 60, 60) {
        didSet {
            retry = Retry(retry: retrySetting.retry, maxRetryCount: retrySetting.maxCount, step: retrySetting.step, fusingDepth: retrySetting.fusingDepth, fusingDuration: retrySetting.fusingDuration)
        }
    }
    public var retrying: Bool { return retry.retrying }
    fileprivate var retry: Retry
    fileprivate var disconnectExpectedly = false
    
    // ssl
    public var enableSSL = false
    public var sslSettings: [String: NSObject]?
    public var allowUntrustCACertificate = false
    
    // subscribed topics. (dictionary structure -> [msgid: [topicString: QoS]])
    var subscriptions: [UInt16: [String: CocoaMQTTQOS]] = [:]
    var subscriptionsWaitingAck: [UInt16: [String: CocoaMQTTQOS]] = [:]
    var unsubscriptionsWaitingAck: [UInt16: [String: CocoaMQTTQOS]] = [:]

    // global message id
    var gmid: UInt16 = 1
    var socket = GCDAsyncSocket()
    fileprivate var reader: CocoaMQTTReader?

    // MARK: init
    public init(clientID: String, host: String = "localhost", port: UInt16 = 1883) {
        self.clientID = clientID
        self.host = host
        self.port = port
        retry = Retry(retry: retrySetting.retry, maxRetryCount: retrySetting.maxCount, step: retrySetting.step, fusingDepth: retrySetting.fusingDepth, fusingDuration: retrySetting.fusingDuration)
        super.init()
        buffer.delegate = self
        printNotice("init")
    }
    
    deinit {
        printNotice("deinit")
        pingpong?.reset()
        retry.reset()
        
        socket.delegate = nil
        socket.disconnect()
    }
    
    internal func buffer(_ buffer: CocoaMQTTFrameBuffer, sendPublishFrame frame: CocoaMQTTFramePublish) {
        send(frame, tag: Int(frame.msgid!))
    }

    fileprivate func send(_ frame: CocoaMQTTFrame, tag: Int = 0) {
        let data = frame.data()
        socket.write(Data(bytes: data, count: data.count), withTimeout: -1, tag: tag)
    }

    fileprivate func sendConnectFrame() {
        let frame = CocoaMQTTFrameConnect(client: self)
        send(frame)
        reader!.start()
        delegate?.mqtt(self, didConnect: host, port: Int(port))
    }

    fileprivate func nextMessageID() -> UInt16 {
        if gmid == UInt16.max {
            gmid = 0
        }
        gmid += 1
        return gmid
    }

    fileprivate func puback(_ type: CocoaMQTTFrameType, msgid: UInt16) {
        var descr: String?
        switch type {
        case .puback:
            descr = "PUBACK"
        case .pubrec:
            descr = "PUBREC"
        case .pubrel:
            descr = "PUBREL"
        case .pubcomp:
            descr = "PUBCOMP"
        default: break
        }

        if descr != nil {
            printDebug("Send \(descr!), msgid: \(msgid)")
        }

        send(CocoaMQTTFramePubAck(type: type, msgid: msgid))
    }

    @discardableResult
    public func connect() -> Bool {
        printNotice("connect")
        retry.reset()
        retry.resetFusing()
        return internal_connect()
    }
    
    @discardableResult
    public func internal_connect() -> Bool {
        printNotice("internal_connect")
        socket.setDelegate(self, delegateQueue: dispatchQueue)
        reader = CocoaMQTTReader(socket: socket, delegate: self)
        do {
            try socket.connect(toHost: self.host, onPort: self.port)
            connState = .connecting
            return true
        } catch let error as NSError {
            printError("socket connect error: \(error.description)")
            return false
        }
    }
    
    /// Only can be called from outside. If you want to disconnect from inside framwork, call internal_disconnect()
    /// disconnect expectedly
    public func disconnect() {
        printNotice("disconnect")
        
        disconnectExpectedly = true
        internal_disconnect()
    }
    
    /// disconnect unexpectedly
    public func internal_disconnect() {
        printNotice("internal_disconnect")
        
        send(CocoaMQTTFrame(type: CocoaMQTTFrameType.disconnect), tag: -0xE0)
        socket.disconnect()
    }
    
    public func ping() {
        printDebug("Ping")
        send(CocoaMQTTFrame(type: CocoaMQTTFrameType.pingreq, payload: [self.presence.rawValue]), tag: -0xC0)
        self.delegate?.mqttDidPing(self)
    }

    @discardableResult
    public func publish(_ topic: String, withString string: String, qos: CocoaMQTTQOS = .qos1, retained: Bool = false, dup: Bool = false) -> UInt16 {
        let message = CocoaMQTTMessage(topic: topic, string: string, qos: qos, retained: retained, dup: dup)
        return publish(message)
    }

    @discardableResult
    public func publish(_ message: CocoaMQTTMessage) -> UInt16 {
        let msgid: UInt16 = nextMessageID()
        let frame = CocoaMQTTFramePublish(msgid: msgid, topic: message.topic, payload: message.payload)
        frame.qos = message.qos.rawValue
        frame.retained = message.retained
        frame.dup = message.dup
//        send(frame, tag: Int(msgid))
        _ = buffer.add(frame)
        
        

        if message.qos != CocoaMQTTQOS.qos0 {
            
        }
        

        delegate?.mqtt(self, didPublishMessage: message, id: msgid)

        return msgid
    }

    @discardableResult
    public func subscribe(_ topic: String, qos: CocoaMQTTQOS = .qos1) -> UInt16 {
        let msgid = nextMessageID()
        let frame = CocoaMQTTFrameSubscribe(msgid: msgid, topic: topic, reqos: qos.rawValue)
        send(frame, tag: Int(msgid))
        subscriptionsWaitingAck[msgid] = [topic:qos]
        return msgid
    }

    @discardableResult
    public func unsubscribe(_ topic: String) -> UInt16 {
        let msgid = nextMessageID()
        let frame = CocoaMQTTFrameUnsubscribe(msgid: msgid, topic: topic)
        unsubscriptionsWaitingAck[msgid] = [topic:CocoaMQTTQOS.qos0]
        send(frame, tag: Int(msgid))
        return msgid
    }
}

// MARK: - GCDAsyncSocketDelegate

extension CocoaMQTT: GCDAsyncSocketDelegate {
    public func socket(_ sock: GCDAsyncSocket, didConnectToHost host: String, port: UInt16) {
        printNotice("AsyncSock connected to \(host) : \(port)")
        
        #if TARGET_OS_IPHONE
            if backgroundOnSocket {
                sock.performBlock { sock.enableBackgroundingOnSocket() }
            }
        #endif
        
        if enableSSL {
            if sslSettings == nil {
                if allowUntrustCACertificate {
                    sock.startTLS([GCDAsyncSocketManuallyEvaluateTrust: true as NSObject]) }
                else {
                    sock.startTLS(nil)
                }
            } else {
                sslSettings![GCDAsyncSocketManuallyEvaluateTrust as String] = NSNumber(value: true)
                sock.startTLS(sslSettings!)
            }
        } else {
            sendConnectFrame()
        }
    }

    public func socket(_ sock: GCDAsyncSocket, didReceive trust: SecTrust, completionHandler: @escaping (Bool) -> Swift.Void) {
        printNotice("AsyncSock didReceiveTrust")
        
        delegate?.mqtt!(self, didReceive: trust, completionHandler: completionHandler)
    }

    public func socketDidSecure(_ sock: GCDAsyncSocket) {
        printNotice("AsyncSock socketDidSecure")
        sendConnectFrame()
    }

    public func socket(_ sock: GCDAsyncSocket, didWriteDataWithTag tag: Int) {
        printDebug("AsyncSock Socket write message with tag: \(tag)")
    }

    public func socket(_ sock: GCDAsyncSocket, didRead data: Data, withTag tag: Int) {
        let etag = CocoaMQTTReadTag(rawValue: tag)!
        var bytes = [UInt8]([0])
        switch etag {
        case CocoaMQTTReadTag.header:
            data.copyBytes(to: &bytes, count: 1)
            reader!.headerReady(bytes[0])
        case CocoaMQTTReadTag.length:
            data.copyBytes(to: &bytes, count: 1)
            reader!.lengthReady(bytes[0])
        case CocoaMQTTReadTag.payload:
            reader!.payloadReady(data)
        }
    }

    public func socketDidDisconnect(_ sock: GCDAsyncSocket, withError err: Error?) {
        printNotice("AsyncSock didDisconnect. Error: \(String(describing: err))")
        
        DispatchQueue.main.async { [weak self] in
            guard let `self` = self else { return }
            
            guard self.disconnectExpectedly == false else { self.retry.reset(); return }
            
            self.retry.start(success: {
                self.handleDisconnect(with: err)
                self.internal_connect()
            }, failure: { (error) in
                self.handleDisconnect(with: error)
            })
        }
    }
    
    private func handleDisconnect(with error: Error?) {
        pingpong?.reset()
        socket.delegate = nil
        socket = GCDAsyncSocket()
        connState = .disconnected
        delegate?.mqttDidDisconnect(self, withError: error)
    }
}

// MARK: - CocoaMQTTReaderDelegate

extension CocoaMQTT: CocoaMQTTReaderDelegate {
    internal func didReceiveConnAck(_ reader: CocoaMQTTReader, connack: UInt8) {
        printNotice("Reader CONNACK Received: \(connack)")

        let ack: CocoaMQTTConnAck
        switch connack {
        case 0:
            ack = .accept
            connState = .connected
        case 1...5:
            ack = CocoaMQTTConnAck(rawValue: connack)!
            internal_disconnect()
        case _ where connack > 5:
            ack = .reserved
            internal_disconnect()
        default:
            internal_disconnect()
            return
        }

        delegate?.mqtt(self, didConnectAck: ack)
        
        if ack == .accept {
            DispatchQueue.main.async { [weak self] in
                self?.retry.reset()
                self?.pingpong?.start()
            }
            disconnectExpectedly = false
        } else {
            pingpong?.reset()
        }
    }

    internal func didReceivePublish(_ reader: CocoaMQTTReader, message: CocoaMQTTMessage, id: UInt16) {
        printDebug("Reader PUBLISH Received from \(message.topic)")
        
        delegate?.mqtt(self, didReceiveMessage: message, id: id)
        switch message.qos {
        case .qos1: puback(CocoaMQTTFrameType.puback, msgid: id)
        case .qos2: puback(CocoaMQTTFrameType.pubrec, msgid: id)
        case .qos0: return
        }
    }

    internal func didReceivePubAck(_ reader: CocoaMQTTReader, msgid: UInt16) {
        printDebug("Reader PUBACK Received: \(msgid)")
        buffer.sendSuccess(withMsgid: msgid)
        delegate?.mqtt(self, didPublishAck: msgid)
    }
    
    internal func didReceivePubRec(_ reader: CocoaMQTTReader, msgid: UInt16) {
        printDebug("Reader PUBREC Received: \(msgid)")
        puback(CocoaMQTTFrameType.pubrel, msgid: msgid)
    }

    internal func didReceivePubRel(_ reader: CocoaMQTTReader, msgid: UInt16) {
        printDebug("Reader PUBREL Received: \(msgid)")
        puback(CocoaMQTTFrameType.pubcomp, msgid: msgid)
    }

    internal func didReceivePubComp(_ reader: CocoaMQTTReader, msgid: UInt16) {
        printDebug("Reader PUBCOMP Received: \(msgid)")
        buffer.sendSuccess(withMsgid: msgid)
        delegate?.mqtt?(self, didPublishComplete: msgid)
    }

    internal func didReceiveSubAck(_ reader: CocoaMQTTReader, msgid: UInt16) {
        if let topicDict = subscriptionsWaitingAck.removeValue(forKey: msgid) {
            let topic = topicDict.first!.key
            
            // remove subscription with same topic
            for (key, value) in subscriptions {
                if value.first!.key == topic {
                    subscriptions.removeValue(forKey: key)
                }
            }
            
            subscriptions[msgid] = topicDict
            printDebug("SUBACK Received: \(topic)")
            delegate?.mqtt(self, didSubscribeTopic: topic)
        } else {
            printWarning("UNEXPECT SUBACK Received: \(msgid)")
        }
    }

    internal func didReceiveUnsubAck(_ reader: CocoaMQTTReader, msgid: UInt16) {
        if let topicDict = unsubscriptionsWaitingAck.removeValue(forKey: msgid) {
            let topic = topicDict.first!.key
            
            for (key, value) in subscriptions {
                if value.first!.key == topic {
                    subscriptions.removeValue(forKey: key)
                }
            }
            
            printDebug("UNSUBACK Received: \(topic)")
            delegate?.mqtt(self, didUnsubscribeTopic: topic)
        } else {
            printWarning("UNEXPECT UNSUBACK Received: \(msgid)")
        }
    }

    internal func didReceivePong(_ reader: CocoaMQTTReader) {
        printDebug("PONG Received")
        pingpong?.pongTime = Date()
        delegate?.mqttDidReceivePong(self)
    }
}

// MARK: - PingPongProtocol

extension CocoaMQTT: PingPongProtocol {
    func pongDidTimeOut() {
        printError("Pong timeout!")
        internal_disconnect()
    }
}

// MAR: - CocoaMQTTReader

internal class CocoaMQTTReader {
    private var socket: GCDAsyncSocket
    private var header: UInt8 = 0
    private var length: UInt = 0
    private var data: [UInt8] = []
    private var multiply = 1
    private var delegate: CocoaMQTTReaderDelegate
    private var timeout = 30000

    init(socket: GCDAsyncSocket, delegate: CocoaMQTTReaderDelegate) {
        self.socket = socket
        self.delegate = delegate
    }

    func start() {
        readHeader()
    }

    func headerReady(_ header: UInt8) {
        printDebug("reader header ready: \(header) ")

        self.header = header
        readLength()
    }

    func lengthReady(_ byte: UInt8) {
        length += (UInt)((Int)(byte & 127) * multiply)
        // done
        if byte & 0x80 == 0 {
            if length == 0 {
                frameReady()
            } else {
                readPayload()
            }
        // more
        } else {
            multiply *= 128
            readLength()
        }
    }

    func payloadReady(_ data: Data) {
        self.data = [UInt8](repeating: 0, count: data.count)
        data.copyBytes(to: &(self.data), count: data.count)
        frameReady()
    }

    private func readHeader() {
        reset()
        socket.readData(toLength: 1, withTimeout: -1, tag: CocoaMQTTReadTag.header.rawValue)
    }

    private func readLength() {
        socket.readData(toLength: 1, withTimeout: TimeInterval(timeout), tag: CocoaMQTTReadTag.length.rawValue)
    }

    private func readPayload() {
        socket.readData(toLength: length, withTimeout: TimeInterval(timeout), tag: CocoaMQTTReadTag.payload.rawValue)
    }

    private func frameReady() {
        // handle frame
        let frameType = CocoaMQTTFrameType(rawValue: UInt8(header & 0xF0))!
        switch frameType {
        case .connack:
            delegate.didReceiveConnAck(self, connack: data[1])
        case .publish:
            let (msgid, message) = unpackPublish()
            if message != nil {
                delegate.didReceivePublish(self, message: message!, id: msgid)
            }
        case .puback:
            delegate.didReceivePubAck(self, msgid: msgid(data))
        case .pubrec:
            delegate.didReceivePubRec(self, msgid: msgid(data))
        case .pubrel:
            delegate.didReceivePubRel(self, msgid: msgid(data))
        case .pubcomp:
            delegate.didReceivePubComp(self, msgid: msgid(data))
        case .suback:
            delegate.didReceiveSubAck(self, msgid: msgid(data))
        case .unsuback:
            delegate.didReceiveUnsubAck(self, msgid: msgid(data))
        case .pingresp:
            delegate.didReceivePong(self)
        default:
            break
        }

        readHeader()
    }

    private func unpackPublish() -> (UInt16, CocoaMQTTMessage?) {
        let frame = CocoaMQTTFramePublish(header: header, data: data)
        frame.unpack()
        // if unpack fail
        if frame.msgid == nil {
            return (0, nil)
        }
        let msgid = frame.msgid!
        let qos = CocoaMQTTQOS(rawValue: frame.qos)!
        let message = CocoaMQTTMessage(topic: frame.topic!, payload: frame.payload, qos: qos, retained: frame.retained, dup: frame.dup)
        return (msgid, message)
    }

    private func msgid(_ bytes: [UInt8]) -> UInt16 {
        if bytes.count < 2 { return 0 }
        return UInt16(bytes[0]) << 8 + UInt16(bytes[1])
    }

    private func reset() {
        length = 0
        multiply = 1
        header = 0
        data = []
    }
}

private extension CocoaMQTT {
    
    // MARK: -- RetryQueue
    
    class RetryQueue {
        let depth: UInt
        let duration: TimeInterval
        
        private var _queue = [Date]()
     
        init(depth: UInt, duration: TimeInterval) {
            self.depth = depth
            self.duration = duration
            
            printNotice("RetryQueue init. Depth: \(depth), duration: \(duration)")
        }
        
        func append(timestamp: Date) throws {
            printDebug("RetryQueue append timestamp: \(timestamp), now queue count: \(_queue.count), queue: \(_queue)")
            
            // remove expired record
            _queue = _queue.filter { $0.addingTimeInterval(duration) >= timestamp }
            
            guard UInt(_queue.count) < depth else {
                throw NSError(domain: "CocoaMQTT-RetryFusing", code: -1, userInfo: ["depth": depth, "duration": duration, "count": _queue.count])
            }
            
            _queue.append(timestamp)
        }
        
        func reset() {
            printNotice("RetryQueue reset.")
            
            _queue = []
        }
    }
    
    // MARK: -- Retry
    
    class Retry {
        let retry: Bool
        let maxRetryCount: UInt
        let step: TimeInterval
        let fusing: RetryQueue
        
        var retriedCount: UInt = 0
        private var timer: Timer? = nil
        private(set) var retrying: Bool = false
        
        // MARK: --- life cycle
        
        init(retry needRetry: Bool, maxRetryCount maxCount: UInt, step s: TimeInterval, fusingDepth: UInt, fusingDuration: TimeInterval) {
            retry = needRetry
            maxRetryCount = maxCount
            step = s
            retrying = false
            
            fusing = RetryQueue(depth: fusingDepth, duration: fusingDuration)
            printNotice("Object.Retry init. retry:\(retry), maxRetryCount:\(maxRetryCount), step:\(step), retrying:\(retrying), fusingDepth: \(fusingDepth), fusingDuration: \(fusingDuration)")
        }
        
        deinit {
            timer?.invalidate()
            timer = nil
            printNotice("Object.Retry deinit.")
        }
        
        // MARK: --- public methods
        
        func start(success: @escaping () -> Void, failure: @escaping (Error) -> Void) {
            guard retrying == false else { return }
            
            printNotice("Object.Retry start")
            
            doRetry(success: success, failure: failure)
        }
        
        func reset() {
            printNotice("Object.Retry reset!")
            
            timer?.invalidate()
            timer = nil
            retriedCount = 0
            retrying = false
        }
        
        func resetFusing() {
            fusing.reset()
        }
        
        // MARK: --- private
        
        private func doRetry(success: @escaping () -> Void, failure: @escaping (Error) -> Void) {
            timer?.invalidate()
            timer = nil
            
            // inner loop
            guard retry && retriedCount < maxRetryCount else {
                retrying = false
                printError("Object.Retry interrupt retrying!")
                failure(NSError(domain: "CocoaMQTT-MaxRetryCount", code: -2, userInfo: ["retry": retry, "retriedCount": retriedCount, "maxRetryCount": maxRetryCount]))
                return
            }
            
            let delay = calcDelay(retried: retriedCount, step: step)
            
            // fusing
            do {
                try fusing.append(timestamp: Date() + delay)
            } catch {
                retrying = false
                printError("Fusing start, error: \(error)")
                failure(error)
                return
            }
            
            retrying = true
            timer = Timer.after(delay) { [weak self] in
                success()
                self?.retriedCount += 1
                printNotice("Object.Retry retriedCount:\(String(describing: self?.retriedCount))")
                self?.doRetry(success: success, failure: failure)
            }
        }
        
        private func calcDelay(retried: UInt, step: TimeInterval) -> TimeInterval {
            let randTime: TimeInterval = Double(arc4random_uniform(UInt32(step * 1000))) / 1000.0
            let delay = Double(retried) * step + randTime
            printDebug("Object.Retry calcDealy:\(delay), retried:\(retried)")
            return delay
        }
    }
    
    // MARK: -- PingPong
    
    class PingPong {
        var pongTime: Date? {
            didSet {
                printDebug("Object.PingPong pongTime:\(String(describing: pongTime))")
                if checkPongExpired(pingTime: pingTime, pongTime: self.pongTime, now: Date(), timeOut: timeInterval) == true {
                    self.delegate?.pongDidTimeOut()
                }
            }
        }
        
        // MARK: --- private
        
        private var pingTime: Date?
        
        private var timer: Timer?
        private let timeInterval: TimeInterval
        private weak var delegate: PingPongProtocol?

        // MARK: --- life cycle
        
        init(keepAlive: TimeInterval, delegate d: PingPongProtocol?) {
            timeInterval = keepAlive
            delegate = d
            printNotice("Object.PingPong init. KeepAlive:\(timeInterval)")
        }
        
        deinit {
            printNotice("Object.PingPong deinit")
            delegate = nil
            reset()
        }
        
        // MARK: --- API
        
        func start() {
            printNotice("Object.PingPong start")
            
            // refresh self's presence status immediately
            delegate?.ping()
            pingTime = Date()
            printDebug("Object.PingPong pingTime:\(String(describing: self.pingTime))")
            
            timer?.invalidate()
            timer = nil
            
            self.timer = Timer.every(timeInterval) { [weak self] (timer: Timer) in
                guard let timeinterval = self?.timeInterval else {
                    printWarning("Object.PingPong timeInterval is nil. \(String(describing: self))")
                    return
                }
                
                let now = Date()
                if self?.checkPongExpired(pingTime: self?.pingTime, pongTime: self?.pongTime, now: now, timeOut: timeinterval) == true {
                    printError("Object.PingPong Pong TIMEOUT! Ping: \(String(describing: self?.pingTime)), Pong: \(String(describing: self?.pongTime)), now: \(now), timeOut: \(timeinterval)")
                    self?.reset()
                    self?.delegate?.pongDidTimeOut()
                } else {
                    self?.delegate?.ping()
                    self?.pingTime = Date()
                    printDebug("Object.PingPong pingTime:\(String(describing: self?.pingTime))")
                }
            }
        }

        func reset() {
            printNotice("Object.PingPong reset")
            timer?.invalidate()
            timer = nil
            pingTime = nil
            pongTime = nil
        }
        
        // MARK: --- private
        
        private func checkPongExpired(pingTime: Date?, pongTime: Date?, now: Date, timeOut: TimeInterval) -> Bool {
            if let pingT = pingTime,
                let pongExpireT = pingTime?.addingTimeInterval(timeOut),
                now > pongExpireT {
                if let pongT = pongTime, pongT >= pingT, pongT <= pongExpireT {
                    return false
                } else {
                    printError("Object.PingPong checkPongExpired. Ping:\(pingT), PongExpired:\(pongExpireT), Now:\(now), Pong:\(String(describing: pongTime))")
                    return true
                }
            } else {
                return false
            }
        }
    }
}

// MARK: - Logger

@objc public enum CocoaMQTTLoggerLevel: Int {
    case debug = 0, info, notice, warning, error
}

extension CocoaMQTTLoggerLevel: CustomStringConvertible {
    public var description: String {
        switch self {
        case .debug: return "Debug"
        case .info: return "Info"
        case .notice: return "Notice"
        case .warning: return "Warning"
        case .error: return "Error"
        }
    }
}

public class CocoaMQTTLogger: NSObject {
    static let shared = CocoaMQTTLogger()
    weak var delegate: CocoaMqttLogProtocol?
    
    public func log(level: CocoaMQTTLoggerLevel, message: String) {
        if let limitLevel = delegate?.minLevel.rawValue,
            level.rawValue >= limitLevel {
            delegate?.log(level: level, message: "CocoaMQTT " + message)
        }
    }
}

func printDebug(_ message: String) {
    CocoaMQTTLogger.shared.log(level: .debug, message: message)
}

func printInfo(_ message: String) {
    CocoaMQTTLogger.shared.log(level: .info, message: message)
}

func printNotice(_ message: String) {
    CocoaMQTTLogger.shared.log(level: .notice, message: message)
}

func printWarning(_ message: String) {
    CocoaMQTTLogger.shared.log(level: .warning, message: message)
}

func printError(_ message: String) {
    CocoaMQTTLogger.shared.log(level: .error, message: message)
}
