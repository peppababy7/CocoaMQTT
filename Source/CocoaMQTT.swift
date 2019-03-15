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

@objc public protocol CocoaMqttLogProtocol: NSObjectProtocol {
    var minLevel: CocoaMQTTLoggerLevel { get }
    func log(level: CocoaMQTTLoggerLevel, message: String)
}

@objc public protocol CocoaMQTTDelegate: CocoaMqttLogProtocol {
    func mqtt(_ mqtt: CocoaMQTT, didConnect host: String, port: Int)
    func mqtt(_ mqtt: CocoaMQTT, didConnectAck ack: CocoaMQTT.ConnAck)
    func mqtt(_ mqtt: CocoaMQTT, didPublishMessage message: CocoaMQTT.Message, id: UInt16)
    func mqtt(_ mqtt: CocoaMQTT, didPublishAck id: UInt16)
    func mqtt(_ mqtt: CocoaMQTT, didReceiveMessage message: CocoaMQTT.Message, id: UInt16 )
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

protocol PingPongProtocol: NSObjectProtocol {
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
public class CocoaMQTT: NSObject, FrameBufferProtocol {
    public var host = "localhost"
    public var port: UInt16 = 1883
    public var clientID: String
    public var username: String?
    public var password: String?
    public var secureMQTT = false
    public var cleanSession = true
    public var willMessage: WillMessage?
    public var presence: UInt8 = 1
    public weak var delegate: CocoaMQTTDelegate? {
        didSet { CocoaMQTTLogger.shared.delegate = delegate }
    }
    public var backgroundOnSocket = false
    public fileprivate(set) var connState = ConnState.initial
    public var dispatchQueue = DispatchQueue.main
    
    // flow control
    var buffer = FrameBuffer()
    
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
    var subscriptions: [UInt16: [String: QOS]] = [:]
    var subscriptionsWaitingAck: [UInt16: [String: QOS]] = [:]
    var unsubscriptionsWaitingAck: [UInt16: [String: QOS]] = [:]

    // global message id
    var gmid: UInt16 = 1
    var socket = GCDAsyncSocket()
    fileprivate var reader: Reader?

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
    
    internal func buffer(_ buffer: FrameBuffer, sendPublishFrame frame: PublishFrame) {
        send(frame, tag: Int(frame.msgid!))
    }

    fileprivate func send(_ frame: Frame, tag: Int = 0) {
        let data = frame.data()
        socket.write(Data(bytes: data, count: data.count), withTimeout: -1, tag: tag)
    }

    fileprivate func sendConnectFrame() {
        let frame = ConnFrame(
            clientId: clientID,
            willMsg: willMessage,
            username: username,
            password: password,
            keepAlive: keepAlive,
            cleanSession: cleanSession
        )
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

    fileprivate func puback(_ type: Frame.TYPE, msgid: UInt16) {
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

        send(CocoaMQTT.PubAckFrame(type: type, msgid: msgid))
    }

    @discardableResult
    public func connect() -> Bool {
        printNotice("connect")
        retry.reset()
        retry.resetFusing()
        pingpong?.reset()
        return internal_connect()
    }
    
    @discardableResult
    public func internal_connect() -> Bool {
        printNotice("internal_connect")
        socket.delegate = nil
        socket = GCDAsyncSocket()
        socket.setDelegate(self, delegateQueue: dispatchQueue)
        reader = Reader(socket: socket, delegate: self)
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
        
        send(Frame(type: Frame.TYPE.disconnect), tag: -0xE0)
        socket.disconnect()
    }
    
    public func ping() {
        printDebug("Ping")
        send(Frame(type: Frame.TYPE.pingreq, payload: [self.presence]), tag: -0xC0)
        self.delegate?.mqttDidPing(self)
    }

    @discardableResult
    public func publish(_ topic: String, withString string: String, qos: QOS = .qos1, retained: Bool = false, dup: Bool = false) -> UInt16 {
        let message = Message(topic: topic, string: string, qos: qos, retained: retained, dup: dup)
        return publish(message)
    }

    @discardableResult
    public func publish(_ message: Message) -> UInt16 {
        let msgid: UInt16 = nextMessageID()
        let frame = PublishFrame(msgid: msgid, topic: message.topic, payload: message.payload)
        frame.qos = message.qos.rawValue
        frame.retained = message.retained
        frame.dup = message.dup
//        send(frame, tag: Int(msgid))
        _ = buffer.add(frame)
        
        

        if message.qos != QOS.qos0 {
            
        }
        

        delegate?.mqtt(self, didPublishMessage: message, id: msgid)

        return msgid
    }

    @discardableResult
    public func subscribe(_ topic: String, qos: QOS = .qos1) -> UInt16 {
        let msgid = nextMessageID()
        let frame = SubscribeFrame(msgid: msgid, topic: topic, reqos: qos.rawValue)
        send(frame, tag: Int(msgid))
        subscriptionsWaitingAck[msgid] = [topic:qos]
        return msgid
    }

    @discardableResult
    public func unsubscribe(_ topic: String) -> UInt16 {
        let msgid = nextMessageID()
        let frame = UnsubscribeFrame(msgid: msgid, topic: topic)
        unsubscriptionsWaitingAck[msgid] = [topic:QOS.qos0]
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
        let etag = Reader.Tag(rawValue: tag)!
        var bytes = [UInt8]([0])
        switch etag {
        case Reader.Tag.header:
            data.copyBytes(to: &bytes, count: 1)
            reader!.headerReady(bytes[0])
        case Reader.Tag.length:
            data.copyBytes(to: &bytes, count: 1)
            reader!.lengthReady(bytes[0])
        case Reader.Tag.payload:
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
        connState = .disconnected
        delegate?.mqttDidDisconnect(self, withError: error)
    }
}

// MARK: - ReaderDelegate

extension CocoaMQTT: ReaderDelegate {
    func reader(_ reader: CocoaMQTT.Reader, didReceiveConnAck connack: UInt8) {
        printNotice("Reader CONNACK Received: \(connack)")
        
        let ack: ConnAck
        switch connack {
        case 0:
            ack = .accept
            connState = .connected
        case 1...5:
            ack = ConnAck(rawValue: connack)!
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

    func reader(_ reader: CocoaMQTT.Reader, didReceivePublish message: CocoaMQTT.Message, id: UInt16) {
        printDebug("Reader PUBLISH Received from \(message.topic)")
        
        delegate?.mqtt(self, didReceiveMessage: message, id: id)
        switch message.qos {
        case .qos1: puback(Frame.TYPE.puback, msgid: id)
        case .qos2: puback(Frame.TYPE.pubrec, msgid: id)
        case .qos0: return
        }
    }

    func reader(_ reader: CocoaMQTT.Reader, didReceivePubAck msgid: UInt16) {
        printDebug("Reader PUBACK Received: \(msgid)")
        buffer.sendSuccess(withMsgid: msgid)
        delegate?.mqtt(self, didPublishAck: msgid)
    }
    
    func reader(_ reader: CocoaMQTT.Reader, didReceivePubRec msgid: UInt16) {
        printDebug("Reader PUBREC Received: \(msgid)")
        puback(Frame.TYPE.pubrel, msgid: msgid)
    }

    func reader(_ reader: CocoaMQTT.Reader, didReceivePubRel msgid: UInt16) {
        printDebug("Reader PUBREL Received: \(msgid)")
        puback(Frame.TYPE.pubcomp, msgid: msgid)
    }

    func reader(_ reader: CocoaMQTT.Reader, didReceivePubComp msgid: UInt16) {
        printDebug("Reader PUBCOMP Received: \(msgid)")
        buffer.sendSuccess(withMsgid: msgid)
        delegate?.mqtt?(self, didPublishComplete: msgid)
    }

    func reader(_ reader: CocoaMQTT.Reader, didReceiveSubAck msgid: UInt16) {
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

    func reader(_ reader: CocoaMQTT.Reader, didReceiveUnsubAck msgid: UInt16) {
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

    internal func didReceivePong(_ reader: Reader) {
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
