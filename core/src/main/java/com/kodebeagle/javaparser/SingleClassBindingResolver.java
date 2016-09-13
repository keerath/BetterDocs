/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.kodebeagle.javaparser;

import com.google.common.collect.Maps;
import com.kodebeagle.javaparser.MethodInvocationResolver.MethodDecl;
import com.kodebeagle.javaparser.MethodInvocationResolver.MethodInvokRef;
import org.eclipse.jdt.core.dom.ASTNode;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import static com.google.common.base.Preconditions.checkNotNull;

public class SingleClassBindingResolver {

    private final ASTNode rootNode;

    private final MethodInvocationResolver resolver = new MethodInvocationResolver();

    public SingleClassBindingResolver(final ASTNode node) {
        rootNode = node;

    }

    /**
     * Return a naive variableName to variableType map.
     *
     * @return
     */
    public Map<String, String> getVariableTypes() {
        final Map<String, String> variableNameTypes = Maps.newTreeMap();
        for (final Entry<Integer, List<ASTNode>> variableBinding : resolver.getVariableBinding()
                .entrySet()) {
            final String varType = checkNotNull(resolver.getVariableTypes()
                    .get(variableBinding.getKey()));
            for (final ASTNode node : variableBinding.getValue()) {
                variableNameTypes.put(node.toString(), varType);
            }
        }
        return variableNameTypes;
    }

    /**
     * Returns the location and type of all the variables.
     *
     * @return
     */
    public Map<ASTNode, String> getVariableTypesAtPosition() {
        final Map<ASTNode, String> variableTypes = Maps.newIdentityHashMap();

        for (final Entry<Integer, List<ASTNode>> variableBinding : resolver.getVariableBinding()
                .entrySet()) {
            Integer bindingId = variableBinding.getKey();
            final String varType = checkNotNull(resolver.getVariableTypes()
                    .get(bindingId));
            for (final ASTNode node : variableBinding.getValue()) {
                variableTypes.put(node, varType);
            }
        }
        return variableTypes;
    }

    public Map<ASTNode, ASTNode> getVariableDependencies() {

        final Map<ASTNode, ASTNode> variableTypes = Maps.newIdentityHashMap();

        for (final Entry<Integer, List<ASTNode>> variableBinding : resolver.getVariableBinding()
                .entrySet()) {
            Integer bindingId = variableBinding.getKey();
            final ASTNode parent = resolver.getVariableDeclarationBinding().get(bindingId);
            for (final ASTNode node : variableBinding.getValue()) {
                variableTypes.put(node, parent);
            }
        }
        return variableTypes;
    }

    /**
     * Returns the locations where a type is mentioned and its actual
     * fully qualified type name.
     *
     * @return
     */
    public Map<ASTNode, String> getTypesAtPosition() {
        final Map<ASTNode, String> nodeTypes = Maps.newIdentityHashMap();

        for (final Entry<ASTNode, String> typeBinding : resolver.getNodeTypeBinding()
                .entrySet()) {
            if (!typeBinding.getValue().contains("<")) {
                nodeTypes.put(typeBinding.getKey(), typeBinding.getValue());
            }
        }
        return nodeTypes;
    }

    public Map<MethodDecl, List<MethodInvokRef>> getMethodInvoks() {
        return resolver.getMethodInvoks();
    }

    public List<MethodDecl> getDeclaredMethods() {
        return resolver.getDeclaredMethods();
    }

    public Map<String, String> getClassesInFile() {
        return resolver.getTypes();
    }

    public List<MethodInvocationResolver.TypeDecl> getTypeDeclarations() {
        return resolver.getTypeDeclarations();
    }

    public Collection<String> getImports() {
        return resolver.getImportedNames().values();
    }

    public Map<ASTNode, String> getImportsDeclarationNode() {
        return resolver.getImportsDeclarationNode();
    }

    public Map<String,String> getSuperType() {
        return resolver.getSuperType();
    }

    public Map<String,List<String>> getInterfaces() {
        return resolver.getInterfaces();
    }

    public Set<MethodInvocationResolver.TypeJavadoc> getTypeJavadocs() { return resolver.getTypeJavadocs(); }

    public void resolve() {
        rootNode.accept(resolver);
    }
}
