<?php
/**
 * beanstalk消费者程序
 * 采用swoole多进程处理
 * 参考：https://wiki.swoole.com/#/process_pool
 */
//引入beanstalk类文件
require_once(__DIR__.'/../lib/Beanstalk.php');
require_once(__DIR__.'/../lib/Common.php');
//定义api返回code常量
define('CODE',[
    1,//接口正常
    -1000//接口异常
]);

//Swoole\Process::daemon();

$workerNum = 2;//开启工作进程数量

$pool = new Swoole\Process\Pool($workerNum);

$pool->on("WorkerStart", function ($pool, $workerId) {
    echo "Worker#{$workerId} is started\n";
    $beanstalk = new Beanstalk();
    //连接beanstalk
    $beanstalk->connect();
    $beanstalk->watch('officialMsg');
    while (true) {
        $data = $beanstalk->reserve();
        $beanstalk->ignore('default');
        try {
            $sendData = json_decode($data['body'], true);
            if ($sendData['method'] === 'post') {
                $res = Common::https_post($sendData['url'], json_encode($sendData['data']), true);
            } else if($sendData['method'] === 'get') {
                $res = Common::https_get($sendData['url']);
            }else{
                $beanstalk->delete($data['id']);
            }
            //curl调用时结果返回为false，说明接口服务器挂掉或服务不可用
            if (false == $res) {
                $beanstalk->release($data['id'], $data['id'], 5);
                echo '接口服务不可用' . PHP_EOL;
            } else {
                //接口返回code
                $code = json_decode($res, true)['code'];
                if (CODE[0] == $code) {
                    $beanstalk->delete($data['id']);
                    echo '该任务执行成功' . PHP_EOL;
                } else {
                    $beanstalk->release($data['id'], $data['id'], 10);
                    echo '该任务不能执行' . PHP_EOL;
                }
            }
        } catch (Exception $e) {
            $beanstalk->release($data['id'], $data['id'], 10);
            echo '该任务不能执行';
        }
    }
});

$pool->on("WorkerStop", function ($pool, $workerId) {
    echo "Worker#{$workerId} is stopped\n";
});

$pool->start();

