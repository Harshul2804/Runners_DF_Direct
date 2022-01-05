import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.Signature;

import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;

public class EncryptionClass {
    public static void main(String args[]) throws Exception{
        //Creating a Signature object
        Signature sign = Signature.getInstance("SHA256withRSA");

        //Creating KeyPair generator object
        KeyPairGenerator keyPairGen = KeyPairGenerator.getInstance("RSA");
        System.out.println("keypair-- "+keyPairGen);

        //Initializing the key pair generator
        keyPairGen.initialize(2048);
        System.out.println("keypair after initialize"+keyPairGen);

        //Generating the pair of keys
        KeyPair pair = keyPairGen.generateKeyPair();
        System.out.println("pair-- "+pair);
        //Creating a Cipher object
        Cipher cipher = Cipher.getInstance("RSA/ECB/PKCS1Padding");
        System.out.println("cipher-- "+cipher);
        //Initializing a Cipher object
        cipher.init(Cipher.ENCRYPT_MODE, pair.getPublic());

        //Adding data to the cipher
        byte[] input = "Welcome to Tutorialspoint".getBytes();
        System.out.println("input-- "+input);
        cipher.update(input);

        //encrypting the data
        byte[] cipherText = cipher.doFinal();
        System.out.println("cipherText-- "+cipherText);
        System.out.println(new String(cipherText, "UTF8"));
    }
}
