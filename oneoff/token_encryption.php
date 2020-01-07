<?php 

$CIPHER = MCRYPT_RIJNDAEL_128;
$MODE = MCRYPT_MODE_CBC;

function generateRandomString($length = 10) {
    $characters = '0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ';
    $charactersLength = strlen($characters);
    $randomString = '';
    for ($i = 0; $i < $length; $i++) {
        $randomString .= $characters[rand(0, $charactersLength - 1)];
    }
    return $randomString;
}

function add_padding($text) {
    $mod = strlen($text) % 3;
    if ($mod <> 0) {
        for ($x = 0; $x < 3-$mod; $x++)
            $text = ' '.$text;
    }
    return $text;
}

function encrypt($plaintext) {
    global $CIPHER, $MODE;
    $key = generateRandomString(16);
    $ivSize = mcrypt_get_iv_size($CIPHER, $MODE);
    $iv = mcrypt_create_iv($ivSize, MCRYPT_RAND);
    $ciphertext = mcrypt_encrypt($CIPHER, $key, $plaintext, $MODE, $iv);
    return base64_encode(add_padding($ciphertext.$iv.$key));
}

function decrypt($ciphertext) {
    global $CIPHER, $MODE;
    $ciphertext = ltrim(base64_decode($ciphertext));
    $ivSize = mcrypt_get_iv_size($CIPHER, $MODE);
    $key = substr($ciphertext, -16);
    $iv = substr($ciphertext, -16-$ivSize, -16);
    $ciphertext = substr($ciphertext, 0, -16-$ivSize);
    $plaintext = mcrypt_decrypt($CIPHER, $key, $ciphertext, $MODE, $iv);
    return $plaintext;
}

// echo generateRandomString(16);
// $ivSize = mcrypt_get_iv_size($CIPHER, $MODE);
// $iv = mcrypt_create_iv($ivSize, MCRYPT_RAND);
// echo $iv;
$ciphertext = encrypt('qckemenpppa|rahasia123');
// $ciphertext = encrypt('devel|rahasiad3v3l');
echo $ciphertext."\n";
echo decrypt($ciphertext)."\n";
// echo decrypt("ICD3AGPkMBciG3YIBJzcAt4HCnKrPAMwJJntHRCmGX2vH8JPRWCXyz4DJpgs0pC61/o1Yng5bmdJQlRHc3BHRnRR")."\n";
?>
