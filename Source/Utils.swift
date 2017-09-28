//
//  Utils.swift
//  CocoaMQTT
//
//  Created by Linda Zhong on 27/09/2017.
//  Copyright © 2017 emqtt.io. All rights reserved.
//

import Cocoa

extension Bool {
    var bit: UInt8 { return self ? 1 : 0 }
}
