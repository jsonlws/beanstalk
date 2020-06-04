<?php
/**
 * Class Common
 * 请求方法公共文件
 */

class Common
{
    /**
     * http post请求方法
     * @param $url
     * @param $data
     * @param bool $is_header
     * @return mixed
     */
    public static function https_post(string $url, string $data, bool $is_header=false):string
    {
        $curl = curl_init();
        curl_setopt($curl, CURLOPT_URL, $url);
        if($is_header) {
            $header  = [
                'Content-Type:'.'application/json; charset=UTF-8'
            ];
            curl_setopt($curl, CURLOPT_HTTPHEADER, $header);
        }
        if (!empty($data)){
            curl_setopt($curl, CURLOPT_POST, 1);
            curl_setopt($curl, CURLOPT_POSTFIELDS, $data);
        }
        curl_setopt($curl, CURLOPT_RETURNTRANSFER, 1);

        if (curl_errno($curl)) {
            return false;
        }
        else{
            $result=curl_exec($curl);
        }
        curl_close($curl);
        if(self::IsJson($result)){
            return $result;
        }else{
            return false;
        }

    }


    /**
     * http get请求方法
     * @param $url
     * @return mixed|string
     */
    public static function https_get(string $url):string
    {
        $curl = curl_init();
        curl_setopt($curl, CURLOPT_URL, $url);
        curl_setopt($curl, CURLOPT_RETURNTRANSFER, TRUE);
        curl_setopt($curl, CURLOPT_SSL_VERIFYPEER, FALSE);
        curl_setopt($curl, CURLOPT_SSL_VERIFYHOST, FALSE);
        curl_setopt($curl, CURLOPT_HEADER, FALSE) ;
        curl_setopt($curl, CURLOPT_TIMEOUT,60);
        if (curl_errno($curl)) {
            return false;
        }
        else{
            $result=curl_exec($curl);
        }
        curl_close($curl);
        if(self::IsJson($result)){
            return $result;
        }else{
            return false;
        }

    }

    /**
     * 解析json串
     * @param string $json_str
     * @return mixed
     */
    public static function IsJson(string $json_str) {
        $json_str = str_replace('＼＼', '', $json_str);
        $out_arr = array();
        preg_match('/{.*}/', $json_str, $out_arr);
        if (!empty($out_arr)) {
            $result = json_decode($out_arr[0], TRUE);
        } else {
            return false;
        }
        return $result;
    }

}