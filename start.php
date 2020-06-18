<?php
/**
 * 项目运行文件
 * User: jsonlws
 * Date: 2020/6/4
 * Time: 15:58
 */

if(!function_exists('exec')){
    echo "exec函数被禁用请解除\n";
    exit();
}

$command = $argv;

$allowCommand = ['start','stop','restart','help'];

if(!isset($command[1])){
    $str = <<<EOL
你可以输入help指令获取命令操作提示!\n
EOL;
    echo $str;
    exit();
}

if(!in_array($command[1],$allowCommand)){
    $str = <<<EOL
你可以输入help指令获取命令操作提示!\n
EOL;
    echo $str;
    exit();
}

$fileList = glob(__DIR__.'/app/*.php',GLOB_BRACE);

if(empty($fileList)){
    $str = <<<EOL
无消费者程序可执行\n
EOL;
    echo $str;
    exit();
}

$noticeMsg = '';

switch ($command[1]){
    case 'help':
        $noticeMsg = <<<EOL
start     启动程序指令\n
restart   重启程序指令\n
stop      停止运行程序\n   
EOL;
        break;
    case 'start':
        foreach ($fileList as $val){
            exec('nohup php '.$val.' >> '.__DIR__.'/log/run.log &');
        }
        $noticeMsg = <<<EOL
程序启动成功\n
EOL;
        break;
    case 'restart':
        foreach ($fileList as $val){
            exec('ps aux |grep '.$val.'|grep -v grep|awk \'{print $2}\'|xargs kill -9');
        }
        foreach ($fileList as $val){
            exec('nohup php '.$val.' >> '.__DIR__.'/log/run.log &');
        }
        $noticeMsg = <<<EOL
程序重启成功\n
EOL;
        break;
    case 'stop':
        foreach ($fileList as $val){
            exec('ps aux |grep '.$val.'|grep -v grep|awk \'{print $2}\'|xargs kill -9');
        }
        $noticeMsg = <<<EOL
程序已经停止\n
EOL;
        break;

}

echo $noticeMsg;
