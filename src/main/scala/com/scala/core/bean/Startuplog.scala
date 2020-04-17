package com.scala.core.bean

/**
 * @Author: Lei
 * @E-mail: 843291011@qq.com
 * @Date: Created in 7:49 上午 2020/4/17
 * @Version: 1.0
 * @Modified By:
 * @Description:
 */
case class Startuplog (mid: String,
                        uid:String,
                        appid:String,
                        area:String,
                        os:String,
                        ch:String,
                        logType:String,
                        vs:String,
                        var logDate:String,
                        var logHour:String,
                        var logHourMinute:String,
                        var ts:Long
                      ) {

}
