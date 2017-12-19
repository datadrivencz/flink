/*
 * Copyright 2017 The Apache Software Foundation.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.contrib.streaming.state;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.operators.testutils.DummyEnvironment;
import org.apache.flink.runtime.state.AbstractKeyedStateBackend;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.runtime.state.internal.InternalListState;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;

import java.io.IOException;

import static java.util.Arrays.asList;

/**
 *
 */
public class RocksDBMapStateTest {

	@Rule
	public TemporaryFolder tempFolder = new TemporaryFolder();

	// Store it because we need it for the cleanup test.
	String dbPath;

	protected RocksDBStateBackend getStateBackend() throws IOException {
		dbPath = tempFolder.newFolder().getAbsolutePath();
		String checkpointPath = tempFolder.newFolder().toURI().toString();
		RocksDBStateBackend backend = new RocksDBStateBackend(
						new FsStateBackend(checkpointPath), true);
		backend.setDbStoragePath(dbPath);
		return backend;
	}

	protected <K> AbstractKeyedStateBackend<K> createKeyedBackend(
					TypeSerializer<K> keySerializer,
					int numberOfKeyGroups,
					KeyGroupRange keyGroupRange,
					Environment env) throws Exception {

		AbstractKeyedStateBackend<K> backend = getStateBackend()
						.createKeyedStateBackend(
										env,
										new JobID(),
										"test_op",
										keySerializer,
										numberOfKeyGroups,
										keyGroupRange,
										env.getTaskKvStateRegistry());

		backend.restore(null);

		return backend;
	}

	protected <K> AbstractKeyedStateBackend<K> createKeyedBackend(
					TypeSerializer<K> keySerializer, Environment env) throws Exception {
		return createKeyedBackend(
						keySerializer,
						10,
						new KeyGroupRange(0, 9),
						env);
	}

	protected <K> AbstractKeyedStateBackend<K> createKeyedBackend(
					TypeSerializer<K> keySerializer) throws Exception {
		return createKeyedBackend(keySerializer, new DummyEnvironment("test", 1, 0));
	}

	@Test
	public void testListStateAddAndGet() throws Exception {

		AbstractKeyedStateBackend<String> keyedBackend = createKeyedBackend(StringSerializer.INSTANCE);

		final ListStateDescriptor<Long> stateDescr = new ListStateDescriptor<>("my-state", Long.class);

		ListState<Long> state =
						keyedBackend.getPartitionedState(VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, stateDescr);

		keyedBackend.setCurrentKey("abc");
		assertNull(state.get());

		keyedBackend.setCurrentKey("def");
		assertNull(state.get());
		state.add(17L);
		state.add(11L);
		assertThat(state.get(), containsInAnyOrder(17L, 11L));
	}

	@Test
	public void testListStateMerging() throws Exception {

		AbstractKeyedStateBackend<String> keyedBackend = createKeyedBackend(StringSerializer.INSTANCE);

		final ListStateDescriptor<Long> stateDescr = new ListStateDescriptor<>("my-state", Long.class);

		final Integer namespace1 = 1;
		final Integer namespace2 = 2;
		final Integer namespace3 = 3;

		try {
			InternalListState<Integer, Long> state =
							(InternalListState<Integer, Long>) keyedBackend.getPartitionedState(0, IntSerializer.INSTANCE, stateDescr);

			// populate the different namespaces
			//  - abc spreads the values over three namespaces
			//  - def spreads teh values over two namespaces (one empty)
			//  - ghi is empty
			//  - jkl has all elements already in the target namespace
			//  - mno has all elements already in one source namespace
			keyedBackend.setCurrentKey("abc");
			state.setCurrentNamespace(namespace1);
			state.add(33L);
			state.add(55L);

			state.setCurrentNamespace(namespace2);
			state.add(22L);
			state.add(11L);

			state.setCurrentNamespace(namespace3);
			state.add(44L);

			keyedBackend.setCurrentKey("def");
			state.setCurrentNamespace(namespace1);
			state.add(11L);
			state.add(44L);

			state.setCurrentNamespace(namespace3);
			state.add(22L);
			state.add(55L);
			state.add(33L);

			keyedBackend.setCurrentKey("jkl");
			state.setCurrentNamespace(namespace1);
			state.add(11L);
			state.add(22L);
			state.add(33L);
			state.add(44L);
			state.add(55L);

			keyedBackend.setCurrentKey("mno");
			state.setCurrentNamespace(namespace3);
			state.add(11L);
			state.add(22L);
			state.add(33L);
			state.add(44L);
			state.add(55L);

			keyedBackend.setCurrentKey("abc");
			state.mergeNamespaces(namespace1, asList(namespace2, namespace3));
			state.setCurrentNamespace(namespace1);
			assertThat(state.get(), containsInAnyOrder(11L, 22L, 33L, 44L, 55L));

			keyedBackend.setCurrentKey("def");
			state.mergeNamespaces(namespace1, asList(namespace2, namespace3));
			state.setCurrentNamespace(namespace1);
			assertThat(state.get(), containsInAnyOrder(11L, 22L, 33L, 44L, 55L));

			keyedBackend.setCurrentKey("ghi");
			state.mergeNamespaces(namespace1, asList(namespace2, namespace3));
			state.setCurrentNamespace(namespace1);
			assertNull(state.get());

			keyedBackend.setCurrentKey("jkl");
			state.mergeNamespaces(namespace1, asList(namespace2, namespace3));
			state.setCurrentNamespace(namespace1);
			assertThat(state.get(), containsInAnyOrder(11L, 22L, 33L, 44L, 55L));

			keyedBackend.setCurrentKey("mno");
			state.mergeNamespaces(namespace1, asList(namespace2, namespace3));
			state.setCurrentNamespace(namespace1);
			assertThat(state.get(), containsInAnyOrder(11L, 22L, 33L, 44L, 55L));

			// make sure all lists / maps are cleared
			keyedBackend.setCurrentKey("abc");
			state.setCurrentNamespace(namespace1);
			state.clear();

			keyedBackend.setCurrentKey("def");
			state.setCurrentNamespace(namespace1);
			state.clear();

			keyedBackend.setCurrentKey("ghi");
			state.setCurrentNamespace(namespace1);
			state.clear();

			keyedBackend.setCurrentKey("jkl");
			state.setCurrentNamespace(namespace1);
			state.clear();

			keyedBackend.setCurrentKey("mno");
			state.setCurrentNamespace(namespace1);
			state.clear();

			assertThat("State backend is not empty.", keyedBackend.numStateEntries(), is(0));
		} finally {
			keyedBackend.close();
			keyedBackend.dispose();
		}
	}
}
