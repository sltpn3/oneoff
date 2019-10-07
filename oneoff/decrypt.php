<?php
$CIPHER = MCRYPT_RIJNDAEL_128;
$MODE = MCRYPT_MODE_CBC;
$key = "uc1N6G3r!n9\0\0\0\0\0";

//$cipher =  fopen("application.ini", "r");
$cipher = file_get_contents("application.ini");

function decrypt($ciphertext) {
        global $CIPHER, $MODE, $key;
        $ciphertext = base64_decode($ciphertext);
        $ivSize = mcrypt_get_iv_size($CIPHER, $MODE);
        if (strlen($ciphertext) < $ivSize) {
            throw new Exception('Missing initialization vector');
        }

        $iv = substr($ciphertext, 0, $ivSize);
        $ciphertext = substr($ciphertext, $ivSize);
        $plaintext = mcrypt_decrypt($CIPHER, $key, $ciphertext, $MODE, $iv);
        return rtrim($plaintext, "\0");
    }

$config = decrypt($cipher);
echo $config;

?>
