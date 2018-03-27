package com.jiuzhong

import java.security.MessageDigest
import java.util
import java.util.Base64

import javax.crypto.Cipher
import javax.crypto.spec.SecretKeySpec
//import org.apache.commons.codec.binary.Base64

class Enc(secret:String,SALT:String) extends java.io.Serializable {
//  private val secret: String = "abcde12345!@#$%"
//  private val SALT: String = "jMhKlOuJnM34G6NHkqo9V010GhLAqOpF0BePojHgh1HgNg8^72k"
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

//object Enc{
//  def encryptFile(enc: Enc,inputFile:String, outFile:PrintWriter): Unit ={
//    try{
//      for (line <- Source.fromFile(inputFile).getLines) {
//        val encStr = enc.encrypt(line)
//        outFile.write(encStr)
//        outFile.write("\n")
//      }
//    }catch {
//      case e: FileNotFoundException => println("Couldn't find that file.")
//      case e: IOException => println("Got an IOException!")
//    }finally {
//      outFile.close()
//    }
//  }
//
//  def decryptFile(enc: Enc,inputFile: String, outFile: PrintWriter): Unit ={
//    try{
//      for (line <- Source.fromFile(inputFile).getLines) {
//        val encStr = enc.decrypt(line)
//        outFile.write(encStr)
//        outFile.write("\n")
//      }
//    }catch {
//      case e: FileNotFoundException => println("Couldn't find that file.")
//      case e: IOException => println("Got an IOException!")
//    }finally {
//      outFile.close()
//    }
//  }
//  def main(args: Array[String]): Unit = {
//    val enc = new Enc()
//    val mode = args(0)
//    val inputFile = args(1)
//    val outFile = new PrintWriter(new File(args(2)))
//
//    if (mode == "0"){
//      encryptFile(enc,inputFile,outFile)
//    }else{
//      decryptFile(enc,inputFile,outFile)
//    }
//
//    //    val inputStr = args(0)
//    //    val outputStr = enc.encrypt(inputStr)
//    //    val encryStr = enc.encrypt(str)
//    //    println(encryStr)
//    //    println(enc.decrypt(encryStr))
//  }
//}
//
