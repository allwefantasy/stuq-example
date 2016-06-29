package com.stuq.chapter02

/**
 * 6/6/16 WilliamZhu(allwefantasy@gmail.com)
 */
object Mock {
  def items = {
    val item = "2016-02-29/09:55:00 GET \"https://domain1/abc/201503/08/23/52/5b1da331305689a3781a181a999458cd_25134600/thumb/2_400_300.jpg\" 200 12 553 30583 TCP_MEM_HIT \"27.43.162.89\" - \"http://m.letv.com/vplay_21344561.html?type=0&id=21344561&ref=baofengyd\" \"image/jpeg\" \"Mozilla/5.0 (Linux; U; Android 4.1.1; zh-cn; MI 2S Build/JRO03L) AppleWebKit/534.30 (KHTML, like Gecko) Version/4.0 Mobile Safari/534.30 storm_browser\" 553 200 *Not IP address [0]*"
    val item2 = "2016-02-29/09:55:00 GET \"https://domain2/abc/201503/08/23/52/5b1da331305689a3781a181a999458cd_25134600/thumb/2_400_300.jpg\" 200 12 553 30583 TCP_MEM_HIT \"27.43.162.89\" - \"http://m.letv.com/vplay_21344561.html?type=0&id=21344561&ref=baofengyd\" \"image/jpeg\" \"Mozilla/5.0 (Linux; U; Android 4.1.1; zh-cn; MI 2S Build/JRO03L) AppleWebKit/534.30 (KHTML, like Gecko) Version/4.0 Mobile Safari/534.30 storm_browser\" 553 200 *Not IP address [0]*"
    val item3 = "2016-02-29/09:55:00 GET \"https://domain3/abc/201503/08/23/52/5b1da331305689a3781a181a999458cd_25134600/thumb/2_400_300.jpg\" 200 12 553 30583 TCP_MEM_HIT \"27.43.162.89\" - \"http://m.letv.com/vplay_21344561.html?type=0&id=21344561&ref=baofengyd\" \"image/jpeg\" \"Mozilla/5.0 (Linux; U; Android 4.1.1; zh-cn; MI 2S Build/JRO03L) AppleWebKit/534.30 (KHTML, like Gecko) Version/4.0 Mobile Safari/534.30 storm_browser\" 553 200 *Not IP address [0]*"
    val item4 = "2016-02-29/09:55:00 GET \"https://domain2/abc/201503/08/23/52/5b1da331305689a3781a181a999458cd_25134600/thumb/2_400_300.jpg\" 200 12 553 30583 TCP_MEM_HIT \"27.43.162.89\" - \"http://m.letv.com/vplay_21344561.html?type=0&id=21344561&ref=baofengyd\" \"image/jpeg\" \"Mozilla/5.0 (Linux; U; Android 4.1.1; zh-cn; MI 2S Build/JRO03L) AppleWebKit/534.30 (KHTML, like Gecko) Version/4.0 Mobile Safari/534.30 storm_browser\" 553 200 *Not IP address [0]*"
    Seq(Seq(item), Seq(item2, item4), Seq(item3), Seq(item4))
  }

  def items2 = {
    val item = "2016-02-29/09:55:00 GET \"https://domain1/abc/201503/08/23/52/5b1da331305689a3781a181a999458cd_25134600/thumb/2_400_300.jpg\" 200 12 553 30583 TCP_MEM_HIT \"27.43.162.89\" - \"http://m.letv.com/vplay_21344561.html?type=0&id=21344561&ref=baofengyd\" \"image/jpeg\" \"Mozilla/5.0 (Linux; U; Android 4.1.1; zh-cn; MI 2S Build/JRO03L) AppleWebKit/534.30 (KHTML, like Gecko) Version/4.0 Mobile Safari/534.30 storm_browser\" 553 200 *Not IP address [0]*"
    val item2 = "2016-02-29/09:55:00 GET \"https://domain2/abc/201503/08/23/52/5b1da331305689a3781a181a999458cd_25134600/thumb/2_400_300.jpg\" 200 12 553 30583 TCP_MEM_HIT \"27.43.162.89\" - \"http://m.letv.com/vplay_21344561.html?type=0&id=21344561&ref=baofengyd\" \"image/jpeg\" \"Mozilla/5.0 (Linux; U; Android 4.1.1; zh-cn; MI 2S Build/JRO03L) AppleWebKit/534.30 (KHTML, like Gecko) Version/4.0 Mobile Safari/534.30 storm_browser\" 553 200 *Not IP address [0]*"
    val item3 = "2016-02-29/09:55:00 GET \"https://domain3/abc/201503/08/23/52/5b1da331305689a3781a181a999458cd_25134600/thumb/2_400_300.jpg\" 200 12 553 30583 TCP_MEM_HIT \"27.43.162.89\" - \"http://m.letv.com/vplay_21344561.html?type=0&id=21344561&ref=baofengyd\" \"image/jpeg\" \"Mozilla/5.0 (Linux; U; Android 4.1.1; zh-cn; MI 2S Build/JRO03L) AppleWebKit/534.30 (KHTML, like Gecko) Version/4.0 Mobile Safari/534.30 storm_browser\" 553 200 *Not IP address [0]*"
    val item4 = "2016-02-29/09:55:00 GET \"https://domain2/abc/201503/08/23/52/5b1da331305689a3781a181a999458cd_25134600/thumb/2_400_300.jpg\" 200 12 553 30583 TCP_MEM_HIT \"27.43.162.89\" - \"http://m.letv.com/vplay_21344561.html?type=0&id=21344561&ref=baofengyd\" \"image/jpeg\" \"Mozilla/5.0 (Linux; U; Android 4.1.1; zh-cn; MI 2S Build/JRO03L) AppleWebKit/534.30 (KHTML, like Gecko) Version/4.0 Mobile Safari/534.30 storm_browser\" 553 200 *Not IP address [0]*"
    Seq(Seq(item, item2, item3, item4))
  }

  def kmeansTraining = {
    Seq(Seq("[0.0, 0.0, 0.0]", "[0.1, 0.1, 0.1]", "[0.2, 0.2, 0.2]", "[9.0, 9.0, 9.0]", "[9.1, 9.1, 9.1]", "[9.2, 9.2, 9.2]"))
  }

  def kmeansPredict = {
    Seq(Seq("(1,[9.2, 9.2, 9.2])"))
  }

}
