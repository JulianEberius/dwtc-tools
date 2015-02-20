package webreduce.fulltext;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.io.IOUtils;
import org.jets3t.service.S3ServiceException;
import org.jets3t.service.ServiceException;
import org.jets3t.service.impl.rest.httpclient.RestS3Service;
import org.jets3t.service.model.S3Object;
import org.jets3t.service.security.AWSCredentials;
import org.jwat.common.RandomAccessFileInputStream;
import org.jwat.gzip.GzipEntry;
import org.jwat.gzip.GzipReader;
import org.jwat.warc.WarcReader;
import org.jwat.warc.WarcReaderFactory;
import org.jwat.warc.WarcRecord;

import webreduce.data.Dataset;

import com.google.common.base.Charsets;
import com.google.common.io.Files;
import com.google.common.io.LineProcessor;

/*
 * Can be used to retrieve to full HTML text that a Dataset (web table) stems from directly
 * from the CommonCrawl on S3.
 */
public class CCFulltext {

	private RestS3Service s3;

	public CCFulltext(String awsKey, String awsSecret)
			throws S3ServiceException {
		super();
		AWSCredentials creds = new AWSCredentials(awsKey, awsSecret);
		s3 = new RestS3Service(creds);
	}

	public String fullText(Dataset er) throws IOException, ServiceException {
		long startOffset = er.getRecordOffset();
		long endOffset = er.getRecordEndOffset();
		String s3Link = er.s3Link;
		return fullText(s3Link, startOffset, endOffset);
	}

	public String fullText(String s3Link, long startOffset, long endOffset)
			throws IOException, ServiceException {
		S3Object inputObject = s3.getObject("aws-publicdatasets", s3Link, null,
				null, null, null, startOffset, endOffset);
		WarcReader warcReader = WarcReaderFactory
				.getReaderCompressed(inputObject.getDataInputStream());
		WarcRecord wr = warcReader.getNextRecord();
		String s = IOUtils.toString(wr.getPayloadContent());
		inputObject.closeDataInputStream();
		warcReader.close();
		return s;
	}

	/* test cases */

	public static void main(String args[]) throws IOException, ServiceException {
		String key = "";
		String secret = "";
		String filePath = "";
		new CCFulltext(key, secret).testLocalExtract(filePath);
	}

	public void testLocalExtract(String filePath) throws IOException,
			ServiceException {
		Files.readLines(new File(filePath), Charset.forName("utf-8"),
				new LineProcessor<String>() {
					private int i = 0;

					@Override
					public String getResult() {
						return null;
					}

					@Override
					public boolean processLine(String s) throws IOException {
						i++;
						if (i != 10)
							return true;
						Dataset er = Dataset.fromJson(s);
						System.out.println(Arrays.deepToString(er.relation));
						try {
							Files.write(fullText(er), new File(
									"/Users/ebi/Desktop/Test" + i + ".html"),
									Charsets.UTF_8);
						} catch (ServiceException e) {
							System.err
									.println("Could not retrieve original HTML from Commoncrawl S3");
						}
						return false;
					}
				});
	}

	public void testS3CC() throws IOException, ServiceException {
		String inputFileKey = "common-crawl/crawl-data/CC-MAIN-2013-20/segments/1368696381249/warc/CC-MAIN-20130516092621-00037-ip-10-60-113-184.ec2.internal.warc.gz";

		S3Object inputObject = s3.getObject("aws-publicdatasets", inputFileKey);

		GzipReader gzipReader = new GzipReader(inputObject.getDataInputStream());
		WarcReader warcReader = WarcReaderFactory.getReaderUncompressed();
		List<Long> offsets = new ArrayList<Long>();
		List<Long> endOffsets = new ArrayList<Long>();
		WarcRecord wr = null;
		String s = "";
		for (int i = 0; i < 100; i++) {
			GzipEntry gzipEntry = gzipReader.getNextEntry();
			long offset = gzipEntry.getStartOffset();
			wr = warcReader.getNextRecordFrom(gzipEntry.getInputStream(),
					offset);
			// gzipreader offset after reading
			String type = wr.getHeader("WARC-Type").value;
			if (type.equals("response")) {
				s = IOUtils.toString(wr.getPayloadContent());
				long endOffset = gzipReader.getOffset();
				offsets.add(offset);
				endOffsets.add(endOffset);
			}
		}
		System.out.println("Last page was:  " + s.substring(0, 2000));

		long startOffset = offsets.get(offsets.size() - 1);
		long endOffset = endOffsets.get(endOffsets.size() - 1);
		System.out.println(String.format(
				"Now retrieving the same page in one go: %d %d", startOffset,
				endOffset));
		S3Object inputObject2 = s3.getObject("aws-publicdatasets",
				inputFileKey, null, null, null, null, startOffset, endOffset);
		GzipReader gzipReader2 = new GzipReader(
				inputObject2.getDataInputStream());
		GzipEntry gzipEntry2 = gzipReader2.getNextEntry();
		System.out.println("gzso: " + gzipEntry2.getStartOffset());
		WarcRecord wr2 = warcReader.getNextRecordFrom(
				gzipEntry2.getInputStream(), gzipEntry2.getStartOffset());
		s = IOUtils.toString(wr2.getPayloadContent());
		System.out.println(String.format("Page at %d was %s", startOffset,
				s.substring(0, 2000)));

		inputObject2.closeDataInputStream();
		gzipReader2.close();

		System.out.println("second reader closed");

		inputObject.closeDataInputStream();
		gzipReader.close();
		warcReader.close();

		System.out.println("first reader closed");
	}

	public void testLocalCC(String filePath) throws IOException,
			ServiceException {
		InputStream in = new FileInputStream(new File(filePath));
		WarcReader warcReader = WarcReaderFactory.getReader(in);
		List<Long> offsets = new ArrayList<Long>();
		List<Long> endOffsets = new ArrayList<Long>();
		WarcRecord wr = null;
		String s = "";
		for (int i = 0; i < 100; i++) {
			wr = warcReader.getNextRecord();
			long offset = warcReader.getStartOffset();
			String type = wr.getHeader("WARC-Type").value;
			if (type.equals("response")) {
				s = IOUtils.toString(wr.getPayloadContent());
				long endOffset = warcReader.getOffset();
				offsets.add(offset);
				endOffsets.add(endOffset);
			}
		}
		System.out.println("Last page was:  " + s.substring(0, 2000));

		long startOffset = offsets.get(offsets.size() - 1);
		long endOffset = endOffsets.get(endOffsets.size() - 1);
		System.out.println(String.format(
				"Now retrieving the same page in one go: %d %d", startOffset,
				endOffset));

		RandomAccessFile f = new RandomAccessFile(filePath, "r");
		RandomAccessFileInputStream in2 = new RandomAccessFileInputStream(f);
		f.seek(startOffset);

		WarcReader warcReader2 = WarcReaderFactory.getReaderCompressed(in2);
		WarcRecord wr2 = warcReader2.getNextRecord();
		s = IOUtils.toString(wr2.getPayloadContent());
		System.out.println(String.format("Page at %d was %s", startOffset,
				s.substring(0, 2000)));

		in2.close();
		warcReader2.close();

		System.out.println("second reader closed");

		in.close();
		warcReader.close();

		System.out.println("first reader closed");
	}
}
