package com.robindrew.trading.histdata.tool.converter;

import static com.robindrew.trading.provider.TradingProvider.HISTDATA;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.util.Collections;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.io.ByteStreams;
import com.robindrew.common.io.Files;
import com.robindrew.common.lang.Args;
import com.robindrew.common.util.Java;
import com.robindrew.trading.histdata.HistDataInstrument;
import com.robindrew.trading.histdata.line.HistDataTickLineParser;
import com.robindrew.trading.price.candle.IPriceCandle;
import com.robindrew.trading.price.candle.format.ptf.source.file.PtfFileManager;
import com.robindrew.trading.price.candle.format.ptf.source.file.PtfFileStreamSink;
import com.robindrew.trading.price.candle.io.list.sink.IPriceCandleListSink;
import com.robindrew.trading.price.candle.io.list.sink.PriceCandleListToStreamSink;
import com.robindrew.trading.price.candle.line.parser.PriceCandleLineFile;

public class HistDataPcfFileConverter {

	private static final Logger log = LoggerFactory.getLogger(HistDataPcfFileConverter.class);

	public static void main(String[] array) {
		Args args = new Args(array);

		// The input directory containing HISTDATA Tick files
		File inputDir = args.getDirectory("-i", true);

		// The output directory for writing PCF files
		File outputDir = args.getDirectory("-o", true);

		HistDataPcfFileConverter converter = new HistDataPcfFileConverter();
		converter.convertInstruments(inputDir, outputDir);
	}

	public void convertInstruments(File inputDir, File outputDir) {
		for (File dir : Files.listFiles(inputDir, false)) {
			try {
				convertInstrument(dir, outputDir);
			} catch (Exception e) {
				log.warn("Exception while converting instrument: " + dir, e);
			}
		}
	}

	public void convertInstrument(File inputDir, File outputDir) {

		// Get the instrument
		String name = inputDir.getName();
		HistDataInstrument instrument = HistDataInstrument.valueOf(name);
		log.info("Converting Instrument: {}", instrument);

		// Output directory
		File directory = PtfFileManager.getDirectory(outputDir, HISTDATA, instrument);
		if (directory.exists()) {
			log.info("Output directory already exists, skipping: {}", directory);
			return;
		}
		directory.mkdirs();

		try (IPriceCandleListSink sink = new PriceCandleListToStreamSink(new PtfFileStreamSink(directory))) {

			// List and sort the files
			List<File> files = Files.listFiles(inputDir, false, ".*\\.zip");
			Collections.sort(files);

			for (File file : files) {
				log.info("Converting File: {}", file);
				File csvTempFile = getCsvTempFile(file);

				HistDataTickLineParser parser = new HistDataTickLineParser(instrument);
				List<IPriceCandle> ticks = new PriceCandleLineFile(csvTempFile, parser).toList();
				sink.putNextCandles(ticks);

				csvTempFile.delete();
			}
		}
	}

	private File getCsvTempFile(File file) {
		try (ZipInputStream input = new ZipInputStream(new FileInputStream(file))) {
			while (true) {
				ZipEntry entry = input.getNextEntry();
				if (entry == null) {
					break;
				}

				if (entry.getName().endsWith(".csv")) {
					File tempFile = File.createTempFile("HISTDATA.", ".csv");
					try (FileOutputStream output = new FileOutputStream(tempFile)) {
						ByteStreams.copy(input, output);
					}
					return tempFile;
				}

			}
			throw new IllegalStateException("CSV file not found in zip file: " + file);

		} catch (Exception e) {
			throw Java.propagate(e);
		}
	}
}
