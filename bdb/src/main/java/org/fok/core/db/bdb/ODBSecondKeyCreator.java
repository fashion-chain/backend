package org.fok.core.db.bdb;

import org.fok.core.dbmodel.Entity.SecondaryValue;

import com.sleepycat.bind.tuple.TupleBinding;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.SecondaryDatabase;
import com.sleepycat.je.SecondaryKeyCreator;

import lombok.AllArgsConstructor;

@AllArgsConstructor
public class ODBSecondKeyCreator implements SecondaryKeyCreator {
	private TupleBinding<SecondaryValue> binding;

	@Override
	public boolean createSecondaryKey(SecondaryDatabase secondary, DatabaseEntry key, DatabaseEntry data,
			DatabaseEntry result) {
		try {
			SecondaryValue v = binding.entryToObject(data);
			if (v.getSecondaryKey() != null && !v.getSecondaryKey().isEmpty()) {
				result.setData(v.getSecondaryKey().toByteArray());
			}
		} catch (Exception e) {
		}
		return false;
	}

}
