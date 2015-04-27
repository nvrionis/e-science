/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package gr.grnet.escience.fs.pithos;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.Serializable;
import gr.grnet.escience.commons.Utils;

/**
 * PithosObject constructor
 */
public class PithosObject implements Serializable {

    private static final long serialVersionUID = 1L;
    private transient PithosBlock[] objectBlocks;
    private String objectName;
    private long totalSize = -1;
    private static final Utils util = new Utils();

    /** Create a Pithos Object **/
    public PithosObject(String name, PithosBlock[] blocks) {
        // - Initialize object name & blocks of the object
        this.objectName = name;
        this.objectBlocks = blocks;
    }

    public String getName() {
        return objectName;
    }

    /**
     * 
     * @return the array of all blocks that comprise the Pithos Object
     */
    public PithosBlock[] getBlocks() {
        return objectBlocks;
    }

    /**
     * 
     * @return the total number of the blocks that comprise the Pithos Object
     */
    public int getBlocksNumber() {
        // - Check if there are available blocks
        if (getBlocks() == null) {
            return getBlocks().length;
        } else {
            return 0;
        }

    }

    /**
     * 
     * @return the total object size in Bytes
     */
    public long getObjectSize() {
        // - Check if there are available blocks
        if (getBlocks() != null) {
            totalSize = 0;

            // - Iterate on all available objects and get the total size in
            // bytes
            for (PithosBlock currentBlock : getBlocks()) {
                totalSize += currentBlock.getBlockLength();
            }
        }
        // - return total size
        return totalSize;
    }

    /****
     * Serialize a Pithos Object so as to perform various actions, such as to
     * copy it to the pithos dfs
     * 
     * @return
     * @throws IOException
     */
    public InputStream serialize() throws IOException {
        // - Create parameters for stream
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(bytes);

        if (getBlocksNumber() > 0) {
            try {
                // - Add object name
                out.write(getName().getBytes());
                // - Add object blocks total number
                out.writeInt(getBlocksNumber());
                // - Add object size
                out.writeLong(getObjectSize());

                // - Add all available blocks for the object
                for (int i = 0; i < getBlocksNumber(); i++) {
                    out.write(getBlocks()[i].getBlockHash().getBytes());
                    out.writeLong(getBlocks()[i].getBlockLength());
                    out.write(getBlocks()[i].getBlockData());
                }
            } finally {
                out.close();
                out = null;
            }
            // - return the inputstream
            return new ByteArrayInputStream(bytes.toByteArray());
        } else {
            return null;
        }

    }

    /***
     * Deserialize a Pithos Object that is received from the pithos dfs
     * 
     * @param {inputStreamForObject: the inputstream that corresponds to
     *        PithosObject bytes}
     * @return
     * @throws IOException
     */
    public static PithosObject deserialize(InputStream inputStreamForObject) {
        if (inputStreamForObject == null) {
            return null;
        }

        InputStream buffer = new BufferedInputStream(inputStreamForObject);
        ObjectInput input = null;

        try {
            input = new ObjectInputStream(buffer);
            return (PithosObject) input.readObject();
        } catch (ClassNotFoundException | IOException e) {
            util.dbgPrint(e.getMessage(), e);
            return null;
        } finally {
            try {
                buffer.close();
                input.close();
            } catch (IOException e) {
                util.dbgPrint(e.getMessage(), e);
            }

        }
    }

}
