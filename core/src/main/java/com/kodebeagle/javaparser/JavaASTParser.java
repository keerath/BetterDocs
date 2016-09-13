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

import org.eclipse.jdt.core.JavaCore;
import org.eclipse.jdt.core.dom.AST;
import org.eclipse.jdt.core.dom.ASTNode;
import org.eclipse.jdt.core.dom.ASTParser;
import org.eclipse.jdt.core.dom.ASTVisitor;
import org.eclipse.jdt.core.dom.MethodDeclaration;

import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Map;

public class JavaASTParser implements Serializable {

    private final boolean useBindings;
    private final boolean useJavadocs;

    public enum ParseType {
        COMPILATION_UNIT, CLASS_BODY, METHOD, STATEMENTS, EXPRESSION
    }

    /**
     * Constructor.
     *
     * @param useBindings calculate bindings on the extracted AST.
     */
    public JavaASTParser(final boolean useBindings) {
        this.useBindings = useBindings;
        useJavadocs = false;
    }

    public JavaASTParser(final boolean useBindings, final boolean useJavadocs) {
        this.useBindings = useBindings;
        this.useJavadocs = useJavadocs;
    }

    /**
     * Get a compilation unit of the given file content.
     *
     * @param fileContent
     * @param parseType
     * @return the compilation unit
     */
    public final ASTNode getAST(final String fileContent,
                                final ParseType parseType) {
        return (ASTNode) getASTNode(fileContent, parseType);
    }

    /**
     * Return an ASTNode given the content
     *
     * @param content
     * @return
     */
    public final ASTNode getASTNode(final char[] content,
                                    final ParseType parseType) {
        final ASTParser parser = ASTParser.newParser(AST.JLS8);
        final int astKind;
        switch (parseType) {
            case CLASS_BODY:
            case METHOD:
                astKind = ASTParser.K_CLASS_BODY_DECLARATIONS;
                break;
            case COMPILATION_UNIT:
                astKind = ASTParser.K_COMPILATION_UNIT;
                break;
            case EXPRESSION:
                astKind = ASTParser.K_EXPRESSION;
                break;
            case STATEMENTS:
                astKind = ASTParser.K_STATEMENTS;
                break;
            default:
                astKind = ASTParser.K_COMPILATION_UNIT;
        }
        parser.setKind(astKind);

        final Map<String, String> options = new HashMap<>();
        options.put(JavaCore.COMPILER_CODEGEN_TARGET_PLATFORM,
                JavaCore.VERSION_1_8);
        options.put(JavaCore.COMPILER_SOURCE, JavaCore.VERSION_1_8);
        if (useJavadocs) {
            options.put(JavaCore.COMPILER_DOC_COMMENT_SUPPORT, JavaCore.ENABLED);
        }
        parser.setCompilerOptions(options);
        parser.setSource(content);
        parser.setResolveBindings(useBindings);
        parser.setBindingsRecovery(useBindings);
        parser.setStatementsRecovery(true);

        if (parseType != ParseType.METHOD) {
            return parser.createAST(null);
        } else {
            final ASTNode cu = parser.createAST(null);
            return getFirstMethodDeclaration(cu);
        }
    }

    /**
     * Get the AST of a string. Path variables cannot be set.
     *
     * @param fileContent
     * @param parseType
     * @return an AST node for the given file content
     * @throws IOException
     */
    public final ASTNode getASTNode(final String fileContent,
                                    final ParseType parseType) {
        return getASTNode(fileContent.toCharArray(), parseType);
    }

    private final MethodDeclaration getFirstMethodDeclaration(final ASTNode node) {
        final TopMethodRetriever visitor = new TopMethodRetriever();
        node.accept(visitor);
        return visitor.getTopDcl();
    }

    private static final class TopMethodRetriever extends ASTVisitor {
        private MethodDeclaration topDcl;

        @Override
        public boolean visit(final MethodDeclaration node) {
            topDcl = node;
            return false;
        }

        public MethodDeclaration getTopDcl() {
            return topDcl;
        }

        public void setTopDcl(MethodDeclaration topDcl) {
            this.topDcl = topDcl;
        }
    }

}
