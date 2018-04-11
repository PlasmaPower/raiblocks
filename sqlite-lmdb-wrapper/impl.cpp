#include <array>
#include <mutex>

#ifdef DEBUG_SQLITE_WRAPPER
#include <iomanip>
#include <iostream>
#endif

#include <boost/filesystem.hpp>
#include <boost/optional.hpp>

#include <sqlite3.h>

#include "lmdb.h"

struct MDB_env
{
	std::string path;
};

int mdb_env_create (MDB_env ** env)
{
	*env = new MDB_env ();
	return 0;
}

int mdb_env_set_maxdbs (MDB_env *, unsigned int dbs)
{
	return 0;
}

int mdb_env_set_mapsize (MDB_env *, size_t size)
{
	return 0;
}

int mdb_env_open (MDB_env * env, const char * path_a, unsigned int flags, uint32_t mode)
{
	boost::system::error_code ec;
	auto path_l (boost::filesystem::canonical (boost::filesystem::path (path_a), ec));
	if (!ec)
	{
		env->path = path_l.string ();
	}
	int result (ec.value ());
	if (!result)
	{
		MDB_txn * txn;
		result = mdb_txn_begin (env, nullptr, 0, &txn);
		if (!result)
		{
			char * errmsg;
			const char * query = "CREATE TABLE IF NOT EXISTS _entry_counts (table NOT NULL UNIQUE TEXT, count NOT NULL UNSIGNED BIG INT);";
			result = sqlite3_exec (txn->db_conn, query, nullptr, nullptr, &errmsg);
#ifdef DEBUG_SQLITE_WRAPPER
			if (result && errmsg)
			{
				std::cerr << "Error creating _entry_counts table: " << std::string (errmsg) << std::endl;
			}
#endif
		}
	}
	return result;
}

int mdb_env_copy2 (MDB_env *, const char * path, unsigned int flags)
{
	// TODO
	return 1;
}

void mdb_env_close (MDB_env * env)
{
	delete env;
}

thread_local std::vector<sqlite3 *> unused_db_connections;

struct MDB_txn
{
	sqlite3 * db_conn;
	std::vector<void *> mdb_values;
	bool can_write;
};

int mdb_txn_begin (MDB_env * env, MDB_txn *, unsigned int flags, MDB_txn ** txn)
{
	int result (0);
	*txn = new MDB_txn ();
#ifdef DEBUG_SQLITE_WRAPPER
	std::cerr << "mdb_txn_begin " << *txn;
#endif
	if (!unused_db_connections.empty ())
	{
		(*txn)->db_conn = unused_db_connections.back ();
#ifdef DEBUG_SQLITE_WRAPPER
		std::cerr << " cached db_conn " << (*txn)->db_conn;
#endif
		unused_db_connections.pop_back ();
	}
	else
	{
		result = sqlite3_open_v2 (env->path.c_str (), &(*txn)->db_conn, SQLITE_OPEN_SHAREDCACHE, nullptr);
		if (!result)
		{
#ifdef DEBUG_SQLITE_WRAPPER
			std::cerr << " opened db_conn " << (*txn)->db_conn;
#endif
			char * errmsg;
			result = sqlite3_exec ((*txn)->db_conn, "PRAGMA journal_mode=WAL;", nullptr, nullptr, &errmsg);
#ifdef DEBUG_SQLITE_WRAPPER
			if (result && errmsg)
			{
				std::cerr << " -> Error enabling WAL: " << std::string (errmsg) << std::endl;
			}
#endif
		}
	}
	if (!result)
	{
		bool read_only = ((flags & MDB_RDONLY) == MDB_RDONLY);
		(*txn)->can_write = !read_only;
		const char * query = read_only ? "BEGIN IMMEDIATE TRANSACTION;" : "BEGIN EXCLUSIVE TRANSACTION;";
#ifdef DEBUG_SQLITE_WRAPPER
		std::cerr << (read_only ? " read-only" : " read-write");
#endif
		char * errmsg;
		result = sqlite3_exec ((*txn)->db_conn, query, nullptr, nullptr, &errmsg);
#ifdef DEBUG_SQLITE_WRAPPER
		if (result && errmsg)
		{
			std::cerr << " -> Error beginning transaction: " << std::string (errmsg) << std::endl;
		}
#endif
	}
#ifdef DEBUG_SQLITE_WRAPPER
	std::cerr << std::endl;
#endif
	return result;
}

int mdb_txn_commit (MDB_txn * txn)
{
	int result (0);
	for (auto & i : txn->mdb_values)
	{
		free (i);
	}
#ifdef DEBUG_SQLITE_WRAPPER
	std::cerr << "mdb_txn_commit " << txn << std::endl;
#endif
	char * errmsg;
	result = sqlite3_exec (txn->db_conn, "COMMIT TRANSACTION;", nullptr, nullptr, &errmsg);
#ifdef DEBUG_SQLITE_WRAPPER
	if (result && errmsg)
	{
		std::cerr << " -> Error committing transaction: " << std::string (errmsg) << std::endl;
	}
#endif
	unused_db_connections.push_back (txn->db_conn);
	delete txn;
	return result;
}

struct MDB_dbi_inner
{
	std::string name;
	bool dups;
};

int mdb_dbi_open (MDB_txn * txn, const char * name_a, unsigned int flags, MDB_dbi * dbi)
{
	std::string name_l (name_a);
#ifdef DEBUG_SQLITE_WRAPPER
	std::cerr << "mdb_dbi_open " << name_l << std::endl;
#endif
	bool dups ((flags & MDB_DUPSORT) == MDB_DUPSORT);
	*dbi = (size_t) new MDB_dbi_inner { name_l, dups };
	sqlite3_stmt * stmt (nullptr);
	const char * query = dups ? "CREATE TABLE IF NOT EXISTS ? (key NOT NULL BLOB, value NOT NULL BLOB);" : \
		"CREATE TABLE IF NOT EXISTS ? (key NOT NULL UNIQUE BLOB, value NOT NULL BLOB);";
	int result (sqlite3_prepare_v2 (txn->db_conn, query, strlen (query) + 1, &stmt, nullptr));
	if (!result)
	{
		result = sqlite3_bind_text (stmt, 1, name_a, strlen (name_a), SQLITE_STATIC);
	}
	if (!result)
	{
		result = sqlite3_step (stmt);
		if (result == SQLITE_DONE)
		{
			result = 0;
		}
	}
	// Can be called at any point in the lifecycle of stmt
	int cleanup_result (sqlite3_finalize (stmt));
	stmt = nullptr;
	if (!result)
	{
		result = cleanup_result;
	}
	if (!result)
	{
		query = "INSERT OR IGNORE INTO _entry_counts (table, count) VALUES (?, 0);";
		result = sqlite3_prepare_v2 (txn->db_conn, query, strlen (query) + 1, &stmt, nullptr);
		if (!result)
		{
			result = sqlite3_bind_text (stmt, 1, name_a, strlen (name_a), SQLITE_STATIC);
		}
		if (!result)
		{
			result = sqlite3_step (stmt);
			if (result == SQLITE_DONE)
			{
				result = 0;
			}
		}
		cleanup_result = sqlite3_finalize (stmt);
		stmt = nullptr;
		if (!result)
		{
			result = cleanup_result;
		}
	}
	return result;
}

void mdb_dbi_close (MDB_env *, MDB_dbi dbi)
{
	delete (MDB_dbi_inner *)dbi;
}

// TODO
/*
int mdb_drop (MDB_txn * txn, MDB_dbi dbi, int del)
{
	int result (0);
	if (!txn->can_write)
	{
		result = EACCES;
	}
	else
	{
#ifdef DEBUG_SQLITE_WRAPPER
		std::cerr << "Emptying DBI " << std::dec << *((std::string *) dbi);
		if (del)
		{
			std::cerr << " (also deleting ID)";
		}
		std::cerr << std::endl;
#endif
		Iterator * it (txn->write_txn->GetIterator (txn->read_opts));
		Slice dbi_slice (Slice ((const char *)&dbi, sizeof (dbi)));
		it->Seek (dbi_slice);
		// Delete all entries
		while (!result && it->Valid ())
		{
			Slice key_slice (it->key ());
			if (key_slice.size () < 2)
			{
				result = MDB_CORRUPTED;
				break;
			}
			else if (*((uint16_t *)key_slice.data ()) != dbi)
			{
				break;
			}
			else
			{
				result = txn->write_txn->Delete (key_slice).code ();
			}
			if (!result)
			{
				result = it->status ().code ();
				it->Next ();
			}
		}
		// Delete ID lookup
		if (del)
		{
			const char dbi_lookup_prefix[] = { 0, 0 };
			if (!result)
			{
				it->Seek (Slice ((const char *)&dbi_lookup_prefix, sizeof (dbi_lookup_prefix)));
			}
			while (!result && it->Valid ())
			{
				Slice key_slice (it->key ());
				if (key_slice.size () < sizeof (dbi_lookup_prefix))
				{
					result = MDB_CORRUPTED;
					break;
				}
				else if (std::memcmp (key_slice.data (), dbi_lookup_prefix, sizeof (dbi_lookup_prefix)))
				{
					assert (false);
					break;
				}
				else if (it->value () == dbi_slice)
				{
					result = txn->write_txn->Delete (key_slice).code ();
					break;
				}
				if (!result)
				{
					result = it->status ().code ();
				}
				if (!result)
				{
					it->Next ();
					assert (it->Valid ());
				}
			}
			if (!result)
			{
				char key_bytes[] = { 0, 0, 0, 0 };
				uint16_t * key_uint16s = (uint16_t *)&key_bytes;
				key_uint16s[0] = ENTRIES_COUNT_PREFIX;
				key_uint16s[1] = dbi;
				Slice key_slice ((const char *)&key_bytes, sizeof (key_bytes));
				result = txn->write_txn->Delete (key_slice).code ();
			}
			mdb_dbi_close (nullptr, dbi);
		}
		delete it;
	}
	return result;
}
*/

int mdb_get (MDB_txn * txn, MDB_dbi dbi, MDB_val * key, MDB_val * value)
{
	sqlite3_stmt * stmt (nullptr);
	const char * query = "SELECT value FROM ? WHERE key = ? ORDER BY key ASC LIMIT 1;";
	int result (sqlite3_prepare_v2 (txn->db_conn, query, strlen (query) + 1, &stmt, nullptr));
	std::string & db_name (((MDB_dbi_inner *)dbi)->name);
	if (!result)
	{
		result = sqlite3_bind_text (stmt, 1, db_name.c_str (), db_name.size () + 1, SQLITE_STATIC);
	}
	if (!result)
	{
		result = sqlite3_bind_text (stmt, 2, (const char *)key->mv_data, key->mv_size, SQLITE_STATIC);
	}
	if (!result)
	{
		result = sqlite3_step (stmt);
		if (result == SQLITE_ROW)
		{
			result = 0;
			value->mv_size = sqlite3_column_bytes (stmt, 0);
			value->mv_data = malloc (value->mv_size);
			txn->mdb_values.push_back (value->mv_data);
			std::memcpy (value->mv_data, sqlite3_column_blob (stmt, 0), value->mv_size);
		}
		else if (result == SQLITE_DONE)
		{
			result = SQLITE_NOTFOUND;
		}
	}
	int cleanup_result (sqlite3_finalize (stmt));
	stmt = nullptr;
	if (!result)
	{
		result = cleanup_result;
	}
#ifdef DEBUG_SQLITE_WRAPPER
	std::cerr << "mdb_get " << txn << " (" << std::dec << dbi << ") ";
	std::cerr << std::hex << std::setfill ('0') << std::setw (0);
	for (size_t i = 0; i < key->mv_size; ++i)
	{
		std::cerr << (uint16_t) (((const uint8_t *)key->mv_data)[i]);
	}
	std::cerr << ": ";
	if (!result)
	{
		for (size_t i = 0; i < value->mv_size; ++i)
		{
			std::cerr << std::hex << (uint16_t) (((const uint8_t *)value->mv_data)[i]);
		}
		std::cerr << std::dec << std::endl;
	}
	else
	{
		std::cerr << "error " << std::dec << result << std::endl;
	}
#endif
	return result;
}

namespace
{
int add_dbi_entries (MDB_txn * txn, const std::string & db_name, int64_t delta)
{
	assert (txn->can_write);
	sqlite3_stmt * stmt (nullptr);
	const char * query = "UPDATE _entry_counts SET count = count + ? WHERE table = ?;";
	int result (sqlite3_prepare_v2 (txn->db_conn, query, strlen (query) + 1, &stmt, nullptr));
	if (!result)
	{
		result = sqlite3_bind_int64 (stmt, 1, delta);
	}
	if (!result)
	{
		result = sqlite3_bind_text (stmt, 2, db_name.c_str (), db_name.size () + 1, SQLITE_STATIC);
	}
	if (!result)
	{
		result = sqlite3_step (stmt);
		if (result == SQLITE_DONE)
		{
			result = 0;
		}
	}
	int cleanup_result (sqlite3_finalize (stmt));
	stmt = nullptr;
	if (!result)
	{
		result = cleanup_result;
	}
	return result;
}

int increment_dbi_entries (MDB_txn * txn, const std::string & db_name)
{
	return add_dbi_entries (txn, db_name, 1);
}

int decrement_dbi_entries (MDB_txn * txn, const std::string & db_name)
{
	return add_dbi_entries (txn, db_name, -1);
}
}

int mdb_put (MDB_txn * txn, MDB_dbi dbi, MDB_val * key, MDB_val * value, unsigned int flags)
{
#ifdef DEBUG_SQLITE_WRAPPER
	std::cerr << "mdb_put " << txn << " (" << std::dec << dbi << ") ";
	for (size_t i = 0; i < key->mv_size; ++i)
	{
		std::cerr << std::hex << (uint16_t) (((const uint8_t *)key->mv_data)[i]);
	}
	std::cerr << ": ";
	for (size_t i = 0; i < value->mv_size; ++i)
	{
		std::cerr << std::hex << (uint16_t) (((const uint8_t *)value->mv_data)[i]);
	}
	std::cerr << std::dec << std::endl;
#endif
	int result (0);
	if (!txn->can_write)
	{
		result = EACCES;
	}
	else
	{
		sqlite3_stmt * stmt (nullptr);
		auto dbi_l ((MDB_dbi_inner *)dbi);
		boost::optional<int64_t> rowid;
		std::string & db_name (dbi_l->name);
		// TODO "INSERT OR REPLACE", then look at sqlite3_changes which does not count REPLACE
		if (!dbi_l->dups)
		{
			const char * query = "SELECT ROWID FROM ? WHERE key = ?;";
			result = sqlite3_prepare_v2 (txn->db_conn, query, strlen (query) + 1, &stmt, nullptr);
			if (!result)
			{
				result = sqlite3_bind_text (stmt, 1, db_name.c_str (), db_name.size () + 1, SQLITE_STATIC);
			}
			if (!result)
			{
				result = sqlite3_bind_blob (stmt, 2, key->mv_data, key->mv_size, SQLITE_STATIC);
			}
			if (!result)
			{
				result = sqlite3_step (stmt);
				if (result == SQLITE_ROW)
				{
					result = 0;
					rowid = sqlite3_column_int64 (stmt, 0);
				}
				else if (result == SQLITE_DONE)
				{
					result = 0;
				}
			}
			int cleanup_result (sqlite3_finalize (stmt));
			stmt = nullptr;
			if (!result)
			{
				result = cleanup_result;
			}
		}
		if (!result && rowid)
		{
			// Update the row
			const char * query = "UPDATE ? SET value = ? WHERE ROWID = ?;";
			result = sqlite3_prepare_v2 (txn->db_conn, query, strlen (query) + 1, &stmt, nullptr);
			if (!result)
			{
				result = sqlite3_bind_text (stmt, 1, db_name.c_str (), db_name.size () + 1, SQLITE_STATIC);
			}
			if (!result)
			{
				result = sqlite3_bind_blob (stmt, 2, value->mv_data, value->mv_size, SQLITE_STATIC);
			}
			if (!result)
			{
				result = sqlite3_bind_int64 (stmt, 3, *rowid);
			}
			if (!result)
			{
				result = sqlite3_step (stmt);
				if (result == SQLITE_DONE)
				{
					result = 0;
				}
			}
		}
		else if (!result)
		{
			// Insert the row
			const char * query = "INSERT INTO ? (key, value) VALUES (?, ?);";
			result = sqlite3_prepare_v2 (txn->db_conn, query, strlen (query) + 1, &stmt, nullptr);
			if (!result)
			{
				result = sqlite3_bind_text (stmt, 1, db_name.c_str (), db_name.size () + 1, SQLITE_STATIC);
			}
			if (!result)
			{
				result = sqlite3_bind_blob (stmt, 2, key->mv_data, key->mv_size, SQLITE_STATIC);
			}
			if (!result)
			{
				result = sqlite3_bind_blob (stmt, 3, value->mv_data, value->mv_size, SQLITE_STATIC);
			}
			if (!result)
			{
				result = sqlite3_step (stmt);
				if (result == SQLITE_DONE)
				{
					result = 0;
				}
			}
			if (!result)
			{
				result = increment_dbi_entries (txn, db_name);
			}
		}
		int cleanup_result (sqlite3_finalize (stmt));
		stmt = nullptr;
		if (!result)
		{
			result = cleanup_result;
		}
	}
	return result;
}

int mdb_del (MDB_txn * txn, MDB_dbi dbi, MDB_val * key, MDB_val * value)
{
#ifdef DEBUG_SQLITE_WRAPPER
	std::cerr << "mdb_del " << txn << " (" << std::dec << dbi << ") ";
	for (size_t i = 0; i < key->mv_size; ++i)
	{
		std::cerr << std::hex << (uint16_t) (((const uint8_t *)key->mv_data)[i]);
	}
	std::cerr << std::endl;
#endif
	int result (0);
	if (!txn->can_write)
	{
		result = EACCES;
	}
	else
	{
		auto dbi_l ((MDB_dbi_inner *)dbi);
		sqlite3_stmt * stmt (nullptr);
		bool match_value (dbi_l->dups && value);
		const char * query = match_value ? "DELETE FROM ? WHERE key = ? AND value = ?;" : "DELETE FROM ? WHERE key = ?;";
		result = sqlite3_prepare_v2 (txn->db_conn, query, strlen (query) + 1, &stmt, nullptr);
		if (!result)
		{
			result = sqlite3_bind_text (stmt, 1, dbi_l->name.c_str (), dbi_l->name.size () + 1, SQLITE_STATIC);
		}
		if (!result)
		{
			result = sqlite3_bind_blob (stmt, 2, key->mv_data, key->mv_size, SQLITE_STATIC);
		}
		if (!result && match_value)
		{
			result = sqlite3_bind_blob (stmt, 3, value->mv_data, value->mv_size, SQLITE_STATIC);
		}
		if (!result)
		{
			result = sqlite3_step (stmt);
			if (result == SQLITE_DONE)
			{
				if (sqlite3_changes (txn->db_conn) == 0)
				{
					result = SQLITE_NOTFOUND;
				}
				else
				{
					result = 0;
				}
			}
		}
		if (!result)
		{
			result = decrement_dbi_entries (txn, dbi_l->name);
		}
		int cleanup_result (sqlite3_finalize (stmt));
		stmt = nullptr;
		if (!result)
		{
			result = cleanup_result;
		}
	}
	return result;
}

struct MDB_cursor
{
	MDB_dbi dbi;
	Iterator * it;
	MDB_txn * txn;
};

int mdb_cursor_open (MDB_txn * txn, MDB_dbi dbi, MDB_cursor ** cursor)
{
	int result = 0;
	*cursor = new MDB_cursor ();
	(*cursor)->dbi = dbi;
	if (txn->write_txn)
	{
		(*cursor)->it = txn->write_txn->GetIterator (txn->read_opts);
	}
	else
	{
		(*cursor)->it = txn->db->NewIterator (txn->read_opts);
	}
	(*cursor)->txn = txn;
	return ((*cursor)->it == nullptr) ? MDB_PANIC : 0;
}

int mdb_cursor_get (MDB_cursor * cursor, MDB_val * key, MDB_val * value, MDB_cursor_op op)
{
	int result (0);
	bool args_output (false);
	switch (op)
	{
		case MDB_GET_CURRENT:
			args_output = true;
			break;
		case MDB_FIRST:
			cursor->it->Seek (Slice ((const char *)&cursor->dbi, sizeof (cursor->dbi)));
			args_output = true;
			break;
		case MDB_SET_RANGE:
		{
			std::vector<uint8_t> ns_key (namespace_key (key, cursor->dbi));
			cursor->it->Seek (Slice ((const char *)ns_key.data (), ns_key.size ()));
			break;
		}
		case MDB_NEXT:
			if (!cursor->it->Valid ())
			{
				result = MDB_NOTFOUND;
			}
			else
			{
				cursor->it->Next ();
			}
			args_output = true;
			break;
		case MDB_NEXT_DUP:
			result = MDB_NOTFOUND;
			break;
	}
	if (!result)
	{
		if (!cursor->it->Valid ())
		{
			result = MDB_NOTFOUND;
		}
		else
		{
			Slice key_slice (cursor->it->key ());
#ifdef DEBUG_SQLITE_WRAPPER
			std::cerr << "Iterator over DBI " << std::dec << cursor->dbi << " at ";
			for (size_t i = 0; i < key_slice.size (); ++i)
			{
				std::cerr << std::hex << (uint16_t)key_slice[i];
			}
			std::cerr << std::dec << std::endl;
#endif
			if (key_slice.size () < 2)
			{
				result = MDB_CORRUPTED;
			}
			else if (*((uint16_t *)key_slice.data ()) != cursor->dbi)
			{
				result = MDB_NOTFOUND;
			}
			if (!result && args_output)
			{
				key->mv_size = key_slice.size () - 2;
				key->mv_data = malloc (key->mv_size);
				cursor->txn->mdb_values.push_back (key->mv_data);
				std::memcpy (key->mv_data, key_slice.data () + 2, key->mv_size);
				Slice value_slice (cursor->it->value ());
				value->mv_size = value_slice.size ();
				value->mv_data = malloc (value->mv_size);
				cursor->txn->mdb_values.push_back (value->mv_data);
				std::memcpy (value->mv_data, value_slice.data (), value->mv_size);
			}
		}
	}
	if (!result)
	{
		result = cursor->it->status ().code ();
	}
	return result;
}

int mdb_cursor_put (MDB_cursor * cursor, MDB_val * key, MDB_val * value, unsigned int flags)
{
	return mdb_put (cursor->txn, cursor->dbi, key, value, flags);
}

void mdb_cursor_close (MDB_cursor * cursor)
{
	delete cursor->it;
	delete cursor;
}

int mdb_stat (MDB_txn * txn, MDB_dbi dbi, MDB_stat * stat)
{
	int result (0);
	char key_bytes[] = { 0, 0, 0, 0 };
	uint16_t * key_uint16s = (uint16_t *)&key_bytes;
	key_uint16s[0] = ENTRIES_COUNT_PREFIX;
	key_uint16s[1] = dbi;
	Slice key ((const char *)&key_bytes, sizeof (key_bytes));
	PinnableSlice value;
	result = txn_get (txn, key, &value).code ();
	if (!result)
	{
		if (value.size () != sizeof (uint64_t))
		{
			result = MDB_CORRUPTED;
		}
		else
		{
			stat->ms_entries = *((const uint64_t *)value.data ());
		}
	}
	return result;
}
