package eu.qwsome.clamav;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.StandardProtocolFamily;
import java.net.UnixDomainSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Lukáš Kvídera
 */
public class ClamavClient {
  private static final Logger LOG = LoggerFactory.getLogger(ClamavClient.class);

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


  public boolean scan(final InputStream inputStream) throws IOException {
    LOG.trace("scan(inputStream)");

    try (final var socketChannel = SocketChannel.open(StandardProtocolFamily.UNIX)) {
      if (socketChannel.connect(this.unixDomainSocketAddress)) {
        final var toClamAVBuffer = ByteBuffer.allocate(4096);
        toClamAVBuffer.put("zINSTREAM".getBytes(StandardCharsets.US_ASCII));

        writeStreamToSocket(inputStream, socketChannel, toClamAVBuffer);
        endStream(toClamAVBuffer, socketChannel);

        return handleResponse(socketChannel);
      }
    }

    throw new IllegalStateException("Socket is in nonblocking mode");
  }


  private void endStream(final ByteBuffer toClamAVBuffer, final SocketChannel socketChannel) throws IOException {
    LOG.trace("endStream(toClamAVBuffer, socketChannel)");

    toClamAVBuffer.clear();
    toClamAVBuffer.putInt(0);
    toClamAVBuffer.flip();

    fullyWrite(toClamAVBuffer, socketChannel);
  }


  private void writeStreamToSocket(
      final InputStream inputStream, final SocketChannel socketChannel, final ByteBuffer toClamAVBuffer
  ) throws IOException {
    LOG.trace("writeStreamToSocket(inputStream, socketChannel, toClamAVBuffer)");

    byte[] batchData;
    while ((batchData = readBatch(inputStream)).length > 0) {
      toClamAVBuffer.put((byte) 0);
      toClamAVBuffer.putInt(batchData.length);
      toClamAVBuffer.put(batchData);
      toClamAVBuffer.flip();

      fullyWrite(toClamAVBuffer, socketChannel);
    }
  }


  private byte[] readBatch(final InputStream inputStream) throws IOException {
    LOG.trace("readBatch(inputStream)");

    final var capacity = 4096;
    var offset = 0;
    final var inputStreamBuffer = new byte[capacity];

    do {
      final var readBytes = inputStream.read(inputStreamBuffer, offset, capacity - offset);
      if (readBytes == -1) {
        if (offset == capacity) {
          return inputStreamBuffer;
        }

        final var trimmedBuffer = new byte[offset];
        if (offset == 0) {
          return trimmedBuffer;
        }

        System.arraycopy(inputStreamBuffer, 0, trimmedBuffer, 0, offset);
        return trimmedBuffer;
      }

      if (readBytes > 0) {
        offset += readBytes;
      }
    } while (offset <= capacity);


    return inputStreamBuffer;
  }


  public boolean scan(final byte[] data) throws IOException {
    LOG.trace("scan(data)");

    try (final var inputStream = new ByteArrayInputStream(data)) {
      return scan(inputStream);
    }
  }


  private boolean handleResponse(final SocketChannel socketChannel) throws IOException {
    LOG.trace("handleResponse(socketChannel)");

    final var completeResponse = new StringBuilder();

    final var responseBuffer = ByteBuffer.allocate(4096);
    while (socketChannel.read(responseBuffer) > 0) {
      responseBuffer.flip();
      final byte[] bytes = new byte[responseBuffer.remaining() - 1];
      responseBuffer.get(bytes);

      completeResponse.append(new String(bytes, StandardCharsets.US_ASCII));
    }


    return completeResponse.toString().equals("stream: OK");
  }


  private void fullyWrite(final ByteBuffer toClamAVBuffer, final SocketChannel socketChannel) throws IOException {
    LOG.trace("fullyWrite(toClamAVBuffer, socketChannel)");

    while (toClamAVBuffer.hasRemaining()) {
      socketChannel.write(toClamAVBuffer);
    }
  }
}
