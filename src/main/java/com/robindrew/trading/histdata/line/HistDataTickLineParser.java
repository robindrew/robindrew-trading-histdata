package com.robindrew.trading.histdata.line;

import static java.time.format.DateTimeFormatter.ofPattern;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.robindrew.common.text.tokenizer.CharTokenizer;
import com.robindrew.trading.histdata.HistDataInstrument;
import com.robindrew.trading.price.candle.IPriceCandle;
import com.robindrew.trading.price.candle.TickPriceCandle;
import com.robindrew.trading.price.decimal.Decimals;

public class HistDataTickLineParser extends HistDataLineParser {

	private static final Logger log = LoggerFactory.getLogger(HistDataTickLineParser.class);

	private static DateTimeFormatter DATE_FORMAT = ofPattern("yyyyMMdd");
	private static DateTimeFormatter TIME_FORMAT = ofPattern("HHmmssSSS");

	private final HistDataInstrument instrument;

	public HistDataTickLineParser(HistDataInstrument instrument) {
		if (instrument == null) {
			throw new NullPointerException("instrument");
		}
		this.instrument = instrument;
	}

	@Override
	public IPriceCandle parseCandle(String line) {
		CharTokenizer tokenizer = new CharTokenizer(line, DELIMITERS);
		int decimalPlaces = instrument.getPricePrecision().getDecimalPlaces();

		// Dates
		LocalDate date = LocalDate.parse(tokenizer.next(false), DATE_FORMAT);
		LocalTime time = LocalTime.parse(tokenizer.next(false), TIME_FORMAT);
		long timestamp = toMillis(LocalDateTime.of(date, time));

		// Prices
		BigDecimal bid = new BigDecimal(tokenizer.next(false));
		BigDecimal ask = new BigDecimal(tokenizer.next(false));
		if (bid.doubleValue() <= 0.0) {
			log.warn("Invalid price candle: '" + line + "'");
			return null;
		}
		if (ask.doubleValue() <= 0.0) {
			log.warn("Invalid price candle: '" + line + "'");
			return null;
		}

		int bidPrice = Decimals.toBigInt(bid, decimalPlaces);
		int askPrice = Decimals.toBigInt(ask, decimalPlaces);

		return new TickPriceCandle(bidPrice, askPrice, timestamp, decimalPlaces);
	}

}
