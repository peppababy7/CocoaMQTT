//
//  CocoaMQTT.swift
//  CocoaMQTT
//
//  Created by Feng Lee<feng@eqmtt.io> on 14/8/3.
//  Copyright (c) 2015 emqtt.io. All rights reserved.
//

import Foundation
import SwiftyTimer
import CocoaAsyncSocket

// MARK: - log protocol

@objc public protocol CocoaMqttLogProtocol: NSObjectProtocol {
    var minLevel: CocoaMQTTLoggerLevel { get }
    func log(level: CocoaMQTTLoggerLevel, message: String)
}

// MARK: - mqtt delegate

@objc public protocol CocoaMQTTDelegate: CocoaMqttLogProtocol {
    func mqtt(_ mqtt: CocoaMQTT, didConnect host: String, port: Int)
    func mqtt(_ mqtt: CocoaMQTT, didConnectAck ack: CocoaMQTTConnAck)
    func mqttDidPing(_ mqtt: CocoaMQTT)
    func mqttDidReceivePong(_ mqtt: CocoaMQTT)
    func mqttDidDisconnect(_ mqtt: CocoaMQTT, withError err: Error?)
    
    func mqtt(_ mqtt: CocoaMQTT, didSubscribeTopic topic: String)
    func mqtt(_ mqtt: CocoaMQTT, didUnsubscribeTopic topic: String)
    func mqtt(_ mqtt: CocoaMQTT, didReceiveMessage message: CocoaMQTTMessage, id: UInt16 )
    func mqtt(_ mqtt: CocoaMQTT, didPublishMessage message: CocoaMQTTMessage, id: UInt16)
    func mqtt(_ mqtt: CocoaMQTT, didPublishAck id: UInt16)
    
    var minLevel: CocoaMQTTLoggerLevel { get }
    func log(level: CocoaMQTTLoggerLevel, message: String)
    
    @objc optional func mqtt(_ mqtt: CocoaMQTT, didReceive trust: SecTrust, completionHandler: @escaping (Bool) -> Void)
    @objc optional func mqtt(_ mqtt: CocoaMQTT, didPublishComplete id: UInt16)
}

// MARK: - public methods

extension CocoaMQTT {

    public func connect() -> Bool {
        printNotice("connect")
        retry.reset()
        retry.resetFusing()
        do {
            try internal_connect()
            return true
        } catch {
            return false
        }
    }

    public func disconnect() {
        printNotice("disconnect")
        
        disconnectExpectedly = true
        internal_disconnect()
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
        _ = buffer.add(frame)
        
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

// MARK: - CocoaMQTT

public class CocoaMQTT: NSObject, CocoaMQTTClient, CocoaMQTTFrameBufferProtocol {
    
    // MARK: -- config setting
    
    public var host = "localhost"
    public var port: UInt16 = 1883
    public var clientID: String
    public var username: String?
    public var password: String?
    public var secureMQTT = false
    public var cleanSession = true
    public var willMessage: CocoaMQTTWill?
    public var presence: CocoaMQTTPresenceType = .away { didSet { ping() } }
    public weak var delegate: CocoaMQTTDelegate? { didSet { CocoaMQTTLogger.shared.delegate = delegate } }
    public var dispatchQueue = DispatchQueue.main
    public fileprivate(set) var connState = CocoaMQTTConnState.initial
    
    // MARK: -- heart beat setting
    
    public var keepAlive: UInt16 = 60 { didSet { pingpong = PingPong(keepAlive: Double(keepAlive / 2 + 1).seconds, delegate: self) } }
    fileprivate var pingpong: PingPong?
    
    // MARK: -- reconnect setting
    
    public var retrySetting: (
        retry: Bool,
        maxCount: UInt,
        step: TimeInterval,
        fusingDepth: UInt,
        fusingDuration: TimeInterval
        ) = (true, 10, 1.2, 60, 60) {
        didSet {
            retry = Retry(
                retry: retrySetting.retry,
                maxRetryCount: retrySetting.maxCount,
                step: retrySetting.step,
                fusingDepth: retrySetting.fusingDepth,
                fusingDuration: retrySetting.fusingDuration
            )
        }
    }
    public var retrying: Bool { return retry.retrying }
    fileprivate var retry: Retry
    fileprivate var disconnectExpectedly = false
    
    // MARK: -- ssl setting
    
    public var enableSSL = false
    public var sslSettings: [String: NSObject] = [:]
    public var allowUntrustCACertificate = false
    
    // MARK: -- life cycle
    
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
        switch type {
            case .puback, .pubrec, .pubrel, .pubcomp:
                printDebug("Send \(type), msgid: \(msgid)")
            default: break
        }

        send(CocoaMQTTFramePubAck(type: type, msgid: msgid))
    }
    
    // MARK: -- private properties
    
    // subscribed topics. (dictionary structure -> [msgid: [topicString: QoS]])
    fileprivate var subscriptions: [UInt16: [String: CocoaMQTTQOS]] = [:]
    fileprivate var subscriptionsWaitingAck: [UInt16: [String: CocoaMQTTQOS]] = [:]
    fileprivate var unsubscriptionsWaitingAck: [UInt16: [String: CocoaMQTTQOS]] = [:]
    
    // global message id
    fileprivate var gmid: UInt16 = 1
    fileprivate var socket = GCDAsyncSocket()
    fileprivate var reader: CocoaMQTTReader?
    
    // flow control
    fileprivate var buffer = CocoaMQTTFrameBuffer()
}

// MARK: -

private extension CocoaMQTT {
    
    func internal_connect() throws {
        printNotice("internal_connect")
        
        socket.setDelegate(self, delegateQueue: dispatchQueue)
        reader = CocoaMQTTReader(socket: socket, delegate: self)
        do {
            try socket.connect(toHost: self.host, onPort: self.port)
            connState = .connecting
        } catch {
            printError("socket connect error: \(error)")
            throw error
        }
    }
    
    func internal_disconnect() {
        printNotice("internal_disconnect")
        
        send(CocoaMQTTFrame(type: CocoaMQTTFrameType.disconnect), tag: -0xE0)
        socket.disconnect()
    }
}

// MARK: - GCDAsyncSocketDelegate

extension CocoaMQTT: GCDAsyncSocketDelegate {
    
    public func socket(_ sock: GCDAsyncSocket, didConnectToHost host: String, port: UInt16) {
        printNotice("AsyncSock connected to \(host) : \(port)")
        switch enableSSL {
        case true:
            sslSettings[GCDAsyncSocketManuallyEvaluateTrust as String] = NSNumber(value: allowUntrustCACertificate)
            sock.startTLS(sslSettings)
            
        case false:
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
        printNotice("AsyncSock didDisconect. Error: \(String(describing: err))")
        
        handleDisconnect(error: err)

        guard self.disconnectExpectedly == false else { self.retry.reset(); return }
        
        self.retry.start(
            success: { [weak self] in
                try? self?.internal_connect()
            },
            failure: { [weak self] (error) in
                self?.handleDisconnect(error: error)
            }
        )
    }
    
    private func handleDisconnect(error: Error?) {
        pingpong?.reset()
        socket.delegate = nil
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
        
        switch ack {
        case .accept:
            retry.reset()
            pingpong?.start()
            disconnectExpectedly = false
            
        default:pingpong?.reset()
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
            printInfo("SUBACK Received: \(topic)")
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
            
            printInfo("UNSUBACK Received: \(topic)")
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
    func ping() {
        printDebug("Ping")
        send(CocoaMQTTFrame(type: CocoaMQTTFrameType.pingreq, payload: [self.presence.rawValue]), tag: -0xC0)
        self.delegate?.mqttDidPing(self)
    }

    func pongDidTimeOut() {
        printError("Pong timeout!")
        internal_disconnect()
    }
}

// MARK: - private

internal protocol PingPongProtocol: NSObjectProtocol {
    func ping()
    func pongDidTimeOut()
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
            guard retrying == false else {
                printWarning("Object.Retry don't retry")
                return
            }
            
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
            printNotice("Object.Retry reset fusing!")
            
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
                printInfo("Object.PingPong pongTime:\(String(describing: pongTime))")
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
            printInfo("Object.PingPong pingTime:\(String(describing: self.pingTime))")
            
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
                    printInfo("Object.PingPong pingTime:\(String(describing: self?.pingTime))")
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

// MARK: - enums

@objc public enum CocoaMQTTQOS: UInt8 {
    case qos0 = 0
    case qos1
    case qos2
}

@objc public enum CocoaMQTTConnState: UInt8 {
    case initial = 0
    case connecting
    case connected
    case disconnected
}

@objc public enum CocoaMQTTConnAck: UInt8 {
    case accept  = 0
    case unacceptableProtocolVersion
    case identifierRejected
    case serverUnavailable
    case badUsernameOrPassword
    case notAuthorized
    case reserved
}

@objc public enum CocoaMQTTPresenceType: UInt8 {
    case away = 0
    case online = 1
}

internal enum CocoaMQTTReadTag: Int {
    case header = 0
    case length
    case payload
}
