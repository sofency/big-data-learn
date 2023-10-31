#!/bin/bash
# 判断参数个数
if [ $# -ge 1 ]
then
# 更新模拟数据的日期
sed -i "/mock.date/c mock.date: $1" ./data-generate-script/application.yml
fi
# 执行脚本
java -jar ./data-generate-script/gmall2020-mock-log-2021-11-29.jar >/dev/null 2&>1 &