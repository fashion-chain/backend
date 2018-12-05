package org.fok.core.db.bdb;

import org.fok.core.db.bdb.model.Entity.SecondaryValue;

import com.sleepycat.bind.tuple.TupleBinding;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.SecondaryDatabase;
import com.sleepycat.je.SecondaryKeyCreator;

import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
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
				return true;
			}
		} catch (Exception e) {
			log.error("", e);
		}
		return false;
	}

}
