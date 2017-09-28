//
//  HeartBeat.swift
//  CocoaMQTT
//
//  Created by Linda Zhong on 28/09/2017.
//  Copyright © 2017 emqtt.io. All rights reserved.
//

import Cocoa
import SwiftyTimer

// MARK: -

protocol PingPongDelegate: NSObjectProtocol {
    func ping()
    func pongDidTimeOut()
}

// MARK: -

class PingPong: NSObject {
    
    // MARK: -- life cycle
    
    init(keepAlive: TimeInterval, delegate: PingPongDelegate) {
        self.timeInterval = keepAlive
        self.delegate = delegate
        printNotice("Object.PingPong init. KeepAlive:\(timeInterval)")
    }
    
    deinit {
        printNotice("Object.PingPong deinit")
        delegate = nil
        reset()
    }
    
    // MARK: -- public methods
    
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
            if self?.expired(pingTime: self?.pingTime, pongTime: self?.pongTime, now: now, timeOut: timeinterval) == true {
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
    
    var pongTime: Date? {
        didSet {
            printInfo("Object.PingPong pongTime:\(String(describing: pongTime))")
            
            if expired(pingTime: pingTime, pongTime: self.pongTime,
                       now: Date(), timeOut: timeInterval) == true
            {
                self.delegate?.pongDidTimeOut()
            }
        }
    }
    
    // MARK: -- private
    
    private func expired(
        pingTime: Date?,
        pongTime: Date?,
        now: Date,
        timeOut: TimeInterval
        ) -> Bool {
        if let pingT = pingTime,
            let pongExpireT = pingTime?.addingTimeInterval(timeOut),
            now > pongExpireT {
            if let pongT = pongTime, pongT >= pingT, pongT <= pongExpireT {
                return false
            } else {
                printError("Object.PingPong expired. Ping:\(pingT), PongExpired:\(pongExpireT), Now:\(now), Pong:\(String(describing: pongTime))")
                return true
            }
        } else {
            return false
        }
    }
    
    // MARK: -- properties
    
    private var pingTime: Date?
    
    private var timer: Timer?
    private let timeInterval: TimeInterval
    private weak var delegate: PingPongDelegate?
}
