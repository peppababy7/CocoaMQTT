//
//  CocoaMQTTLog.swift
//  CocoaMQTT
//
//  Created by Linda Zhong on 21/09/2017.
//  Copyright © 2017 emqtt.io. All rights reserved.
//

import Foundation

protocol LogProtocol: NSObjectProtocol {
    var minLevel: Log.Level { get }
    func log(level: Log.Level, message: String)
}

class Log: NSObject {
    
    static var shared: Log = Log()
    
    enum Level: Int {
        case debug = 0
        case info
        case notice
        case warning
        case error
    }
    
    func log(level: Level, message: String) {
        guard let minlevel = delegate?.minLevel, level.rawValue >= minlevel.rawValue else { return }
        
        delegate?.log(level: level, message: message)
    }
    
    weak var delegate: LogProtocol?
}

func printDebug(_ message: String) { Log.shared.log(level: .debug, message: message) }

func printInfo(_ message: String) { Log.shared.log(level: .info, message: message) }

func printNotice(_ message: String) { Log.shared.log(level: .notice, message: message) }

func printWarning(_ message: String) { Log.shared.log(level: .warning, message: message) }

func printError(_ message: String) { Log.shared.log(level: .error, message: message) }
