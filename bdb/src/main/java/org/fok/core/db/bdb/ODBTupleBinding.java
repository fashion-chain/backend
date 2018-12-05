package org.fok.core.db.bdb;

import org.fok.core.db.bdb.model.Entity.SecondaryValue;

import com.sleepycat.bind.tuple.TupleBinding;
import com.sleepycat.bind.tuple.TupleInput;
import com.sleepycat.bind.tuple.TupleOutput;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ODBTupleBinding extends TupleBinding<SecondaryValue> {

	@Override
	public SecondaryValue entryToObject(TupleInput input) {
		byte bb[] = new byte[input.available()];
		input.read(bb);
		try {
			return SecondaryValue.parseFrom(bb);
		} catch (Exception e) {
			log.error("", e);
			return null;
		}
	}

	@Override
	public void objectToEntry(SecondaryValue object, TupleOutput output) {
		output.write(object.toByteArray());
	}
}
