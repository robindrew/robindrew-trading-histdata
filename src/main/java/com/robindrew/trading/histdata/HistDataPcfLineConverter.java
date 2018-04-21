package com.robindrew.trading.histdata;

import com.robindrew.trading.histdata.line.HistDataLineFilter;
import com.robindrew.trading.histdata.line.HistDataM1LineParser;
import com.robindrew.trading.histdata.line.HistDataTickLineParser;
import com.robindrew.trading.price.candle.format.pcf.source.IPcfSourceManager;
import com.robindrew.trading.price.candle.format.pcf.source.file.PcfFileLineConverter;
import com.robindrew.trading.price.candle.line.filter.ILineFilter;
import com.robindrew.trading.price.candle.line.parser.IPriceCandleLineParser;

public class HistDataPcfLineConverter extends PcfFileLineConverter {

	public static HistDataPcfLineConverter createM1(IPcfSourceManager manager, HistDataInstrument instrument) {
		IPriceCandleLineParser parser = new HistDataM1LineParser(instrument);
		ILineFilter filter = new HistDataLineFilter();
		HistDataPcfLineConverter converter = new HistDataPcfLineConverter(manager, parser, filter);
		return converter;
	}

	public static HistDataPcfLineConverter createTick(IPcfSourceManager manager, HistDataInstrument instrument) {
		IPriceCandleLineParser parser = new HistDataTickLineParser(instrument);
		ILineFilter filter = new HistDataLineFilter();
		return new HistDataPcfLineConverter(manager, parser, filter);
	}

	public HistDataPcfLineConverter(IPcfSourceManager manager, IPriceCandleLineParser parser, ILineFilter filter) {
		super(manager, parser, filter);
	}

}
