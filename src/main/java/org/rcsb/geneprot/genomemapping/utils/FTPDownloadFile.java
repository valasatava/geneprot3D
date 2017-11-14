package org.rcsb.geneprot.genomemapping.utils;

import org.apache.commons.net.ftp.FTP;
import org.apache.commons.net.ftp.FTPClient;

import java.io.*;

/**
 * A program demonstrates how to upload files from local computer to a remote
 * FTP server using Apache Commons Net API.
 * @author www.codejava.net
 */
public class FTPDownloadFile {
 
    public static void main(String[] args) throws Exception {

    }

    public static void download(String server, int port, String user, String pass, String remoteFile, String downloadFile) throws Exception {

        FTPClient ftpClient = new FTPClient();
        try {
 
            ftpClient.connect(server, port);
            ftpClient.login(user, pass);
            ftpClient.enterLocalPassiveMode();
            ftpClient.setFileType(FTP.BINARY_FILE_TYPE);

            OutputStream outputStream = new BufferedOutputStream(new FileOutputStream(new File(downloadFile)));
            boolean success = ftpClient.retrieveFile(remoteFile, outputStream);
            outputStream.close();
            if (success) {
                System.out.println("File has been downloaded successfully.");
            }

        } catch (IOException ex) {
            System.out.println("Error: " + ex.getMessage());
            ex.printStackTrace();
        } finally {
            try {
                if (ftpClient.isConnected()) {
                    ftpClient.logout();
                    ftpClient.disconnect();
                }
            } catch (IOException ex) {
                ex.printStackTrace();
            }
        }
    }
}