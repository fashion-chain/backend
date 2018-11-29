package org.fok.core.db.bdb;

import org.fok.core.dbmodel.Entity.SecondaryValue;

import com.sleepycat.bind.tuple.TupleBinding;
import com.sleepycat.bind.tuple.TupleInput;
import com.sleepycat.bind.tuple.TupleOutput;

public class ODBTupleBinding extends TupleBinding<SecondaryValue> {

	@Override
	public SecondaryValue entryToObject(TupleInput input) {
		byte bb[] = new byte[input.available()];
		input.read(bb);
		try {
			return SecondaryValue.parseDelimitedFrom(input);
		} catch (Exception e) {
			return null;
		}
	}

	@Override
	public void objectToEntry(SecondaryValue object, TupleOutput output) {
		output.write(object.toByteArray());
	}
}
