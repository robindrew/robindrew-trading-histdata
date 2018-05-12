package com.robindrew.trading.histdata.tool;

import static com.robindrew.trading.histdata.line.HistDataFormat.TICK;
import static com.robindrew.trading.provider.TradingProvider.HISTDATA;
import static java.util.concurrent.TimeUnit.HOURS;

import java.io.File;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.robindrew.common.io.Files;
import com.robindrew.common.lang.Args;
import com.robindrew.common.util.Check;
import com.robindrew.common.util.Threads;
import com.robindrew.trading.histdata.HistDataInstrument;
import com.robindrew.trading.histdata.HistDataPcfLineConverter;
import com.robindrew.trading.histdata.line.HistDataFormat;
import com.robindrew.trading.histdata.line.HistDataLineFilter;
import com.robindrew.trading.histdata.line.HistDataM1LineParser;
import com.robindrew.trading.histdata.line.HistDataTickLineParser;
import com.robindrew.trading.price.candle.format.pcf.source.IPcfSourceProviderManager;
import com.robindrew.trading.price.candle.format.pcf.source.file.PcfFileProviderManager;
import com.robindrew.trading.price.candle.line.filter.ILineFilter;
import com.robindrew.trading.price.candle.line.parser.IPriceCandleLineParser;
import com.robindrew.trading.provider.TradingProvider;

public class HistDataPcfConverterTool implements Runnable {

	public static void main(String[] array) {
		Args args = new Args(array);

		File inputDir = args.getDirectory("-i", true);
		File outputDir = args.getDirectory("-o", true);
		HistDataFormat format = args.getEnum("-f", HistDataFormat.class, TICK);
		TradingProvider provider = args.getEnum("-p", TradingProvider.class, HISTDATA);

		IPcfSourceProviderManager manager = new PcfFileProviderManager(outputDir, provider);

		new HistDataPcfConverterTool(inputDir, manager, format).run();;
	}

	private final File inputDir;
	private final IPcfSourceProviderManager manager;
	private final HistDataFormat format;
	private final ExecutorService executor = Executors.newFixedThreadPool(5);

	public HistDataPcfConverterTool(File inputDir, IPcfSourceProviderManager manager, HistDataFormat format) {
		this.inputDir = Check.existsDirectory("inputDir", inputDir);
		this.manager = Check.notNull("manager", manager);
		this.format = Check.notNull("format", format);
	}

	@Override
	public void run() {
		convertFiles();
	}

	private void convertFiles() {
		for (File instrumentDir : Files.listContents(inputDir)) {
			executor.execute(() -> convertDirectory(instrumentDir));
		}
		Threads.shutdownService(executor, 1, HOURS);
	}

	private void convertDirectory(File fromDirectory) {

		HistDataInstrument instrument = HistDataInstrument.valueOf(fromDirectory.getName());
		IPriceCandleLineParser parser = createParser(format, instrument);
		ILineFilter filter = new HistDataLineFilter();
		HistDataPcfLineConverter converter = new HistDataPcfLineConverter(manager, parser, filter);
		converter.convert(instrument, fromDirectory);
	}

	private IPriceCandleLineParser createParser(HistDataFormat format2, HistDataInstrument instrument) {
		switch (format) {
			case M1:
				return new HistDataM1LineParser(instrument);
			case TICK:
				return new HistDataTickLineParser(instrument);
			default:
				throw new IllegalArgumentException("Format not supported: " + format);
		}
	}
}
