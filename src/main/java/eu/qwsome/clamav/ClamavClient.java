package eu.qwsome.clamav;

import java.io.IOException;
import java.net.StandardProtocolFamily;
import java.net.UnixDomainSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;

/**
 * @author Lukáš Kvídera
 */
public class ClamavClient {

  private final UnixDomainSocketAddress unixDomainSocketAddress;


  private ClamavClient(final UnixDomainSocketAddress unixDomainSocketAddress) {
    this.unixDomainSocketAddress = unixDomainSocketAddress;
  }


  public static ClamavClient useSocket(final String unixSocketPath) {
    return useSocket(Path.of(unixSocketPath));
  }


  public static ClamavClient useSocket(final Path unixSocketPath) {
    return new ClamavClient(UnixDomainSocketAddress.of(unixSocketPath));
  }


  public boolean scan(final byte[] data) throws IOException {
    try (final var socketChannel = SocketChannel.open(StandardProtocolFamily.UNIX)) {
      if (socketChannel.connect(this.unixDomainSocketAddress)) {
        final var toClamAVBuffer = ByteBuffer.allocate(4096);
        toClamAVBuffer.put("zINSTREAM".getBytes(StandardCharsets.US_ASCII));
        toClamAVBuffer.put((byte) 0);
        toClamAVBuffer.putInt(data.length);
        toClamAVBuffer.put(data);
        toClamAVBuffer.flip();

        fullyWrite(toClamAVBuffer, socketChannel);

        toClamAVBuffer.clear();
        toClamAVBuffer.putInt(0);
        toClamAVBuffer.flip();

        fullyWrite(toClamAVBuffer, socketChannel);

        if (handleResponse(socketChannel)) {
          System.out.println("OK");
        } else {
          System.out.println("ERR");
        }
      }
    }
    return false;
  }


  private boolean handleResponse(final SocketChannel socketChannel) throws IOException {
    var completeResponse = new StringBuilder();

    final var responseBuffer = ByteBuffer.allocate(4096);
    while (socketChannel.read(responseBuffer) > 0) {
      responseBuffer.flip();
      byte[] bytes = new byte[responseBuffer.remaining() - 1];
      responseBuffer.get(bytes);

      completeResponse.append(new String(bytes, StandardCharsets.US_ASCII));
    }


    return completeResponse.toString().equals("stream: OK");
  }


  private void fullyWrite(final ByteBuffer toClamAVBuffer, final SocketChannel socketChannel) throws IOException {
    while (toClamAVBuffer.hasRemaining()) {
      socketChannel.write(toClamAVBuffer);
    }
  }
}
