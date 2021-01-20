| TipsID | Tips              | Why                                                                            | Note |
|--------|-------------------|--------------------------------------------------------------------------------|------|
| 1      | Use UDF carefully | Pyspark UDF是透過Python計算，而內建函數是Java計算，兩者之間有效能差異，UDF易成為bottleneck |      |
| 2      | Use UI monitor | monitor可以觀察到spark怎麼重新優化你的Ma, pReduce(Transformation, Action)，幫助尋找bottleneck以及邏輯錯誤原因 |      |
