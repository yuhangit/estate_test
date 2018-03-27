package com.jiuzhong

import java.security.MessageDigest
import java.util
import java.util.Base64

import javax.crypto.Cipher
import javax.crypto.spec.SecretKeySpec
//import org.apache.commons.codec.binary.Base64

class Enc(secret:String,SALT:String) extends java.io.Serializable {
  private val keyTOSpec = keyToSpec(secret)

  def encrypt(value: String): String = {
    val cipher: Cipher = Cipher.getInstance("AES/ECB/PKCS5Padding")
    cipher.init(Cipher.ENCRYPT_MODE, keyTOSpec)
    new String(Base64.getEncoder.encode(cipher.doFinal(value.getBytes("UTF-8"))))
  }

  def keyToSpec(key: String): SecretKeySpec = {
    var keyBytes: Array[Byte] = (SALT + key).getBytes("UTF-8")
    val sha: MessageDigest = MessageDigest.getInstance("SHA-1")
    keyBytes = sha.digest(keyBytes)
    keyBytes = util.Arrays.copyOf(keyBytes, 16)
    new SecretKeySpec(keyBytes, "AES")
  }

  def decrypt(value: String):String = {
    val key = secret
    val cipher: Cipher = Cipher.getInstance("AES/ECB/PKCS5Padding")
    cipher.init(Cipher.DECRYPT_MODE, keyTOSpec)
    new String(cipher.doFinal(Base64.getDecoder.decode(value.getBytes())))
  }
}