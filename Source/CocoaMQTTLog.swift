//
//  CocoaMQTTLog.swift
//  CocoaMQTT
//
//  Created by Linda Zhong on 21/09/2017.
//  Copyright © 2017 emqtt.io. All rights reserved.
//

import Foundation

public enum CocoaMQTTLoggerLevel: Int {
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
