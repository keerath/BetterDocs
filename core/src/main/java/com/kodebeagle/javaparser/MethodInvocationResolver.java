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

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import org.eclipse.jdt.core.dom.ASTNode;
import org.eclipse.jdt.core.dom.AbstractTypeDeclaration;
import org.eclipse.jdt.core.dom.Assignment;
import org.eclipse.jdt.core.dom.ClassInstanceCreation;
import org.eclipse.jdt.core.dom.EnumDeclaration;
import org.eclipse.jdt.core.dom.Expression;
import org.eclipse.jdt.core.dom.Javadoc;
import org.eclipse.jdt.core.dom.MethodDeclaration;
import org.eclipse.jdt.core.dom.MethodInvocation;
import org.eclipse.jdt.core.dom.SimpleName;
import org.eclipse.jdt.core.dom.SingleVariableDeclaration;
import org.eclipse.jdt.core.dom.Type;
import org.eclipse.jdt.core.dom.VariableDeclarationFragment;
import org.eclipse.jdt.core.dom.VariableDeclarationStatement;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;

public class MethodInvocationResolver extends TypeResolver {

    private static final String OBJECT_TYPE = "java.lang.Object";

    private Map<MethodDecl, List<MethodInvokRef>> methodInvoks = new HashMap<>();
    private Stack<MethodDeclaration> methodStack = new Stack<MethodDeclaration>();
    private List<MethodDecl> declaredMethods = new ArrayList<MethodDecl>();
    private List<TypeDecl> typeDeclarations = new ArrayList<>();
    protected Map<String, String> types = new HashMap<>();
    protected Stack<String> typesInFile = new Stack<>();
    protected Map<String, String> typeTosuperType = new HashMap<>();
    private Map<String, List<Object>> typeToInterfacesFullyQualifiedName = new HashMap<>();
    public Map<String, TypeJavadoc> typeJavadocs = new HashMap<>();

    public Map<String, String> getSuperType() {
        return typeTosuperType;
    }

    public List<TypeDecl> getTypeDeclarations() {
        return typeDeclarations;
    }

    public Map<MethodDecl, List<MethodInvokRef>> getMethodInvoks() {
        return methodInvoks;
    }

    public List<MethodDecl> getDeclaredMethods() {
        return declaredMethods;
    }

    public Set<TypeJavadoc> getTypeJavadocs() {

        Set<TypeJavadoc> typeJavadocsSet = new HashSet<>();

        for (String key : this.typeJavadocs.keySet()) {

            typeJavadocsSet.add(this.typeJavadocs.get(key));

        }

        return typeJavadocsSet;
    }

    private String removeSpecialSymbols(final String pType) {
        String type = pType;
        if (type != null && type.contains("<")) {
            type = type.substring(0, type.indexOf("<"));
        } else if (type != null && type.contains("[")) {
            type = type.substring(0, type.indexOf("["));
        }
        return type;
    }

    @Override
    public boolean visit(org.eclipse.jdt.core.dom.TypeDeclaration td) {
        String typeFullyQualifiedName = removeSpecialSymbols(td.getName().getFullyQualifiedName());
        if (td.getSuperclassType() != null) {
            typeTosuperType.put(typeFullyQualifiedName,
                    getFullyQualifiedNameFor(removeSpecialSymbols(td.getSuperclassType().toString())));
        }
        if (td.superInterfaceTypes() != null && td.superInterfaceTypes().size() > 0) {
            List<Object> interfaces = td.superInterfaceTypes();
            typeToInterfacesFullyQualifiedName.put(typeFullyQualifiedName, interfaces);
        }
        typesInFile.push(td.getName().getFullyQualifiedName());
        TypeDecl obj = new TypeDecl(typeFullyQualifiedName, td.getName().getStartPosition());
        typeDeclarations.add(obj);

        addTypeDoc(td, typeFullyQualifiedName);
        return true;
    }

    public Map<String, String> getTypes() {
        return types;
    }

    @Override
    public void endVisit(org.eclipse.jdt.core.dom.TypeDeclaration td) {
        if (!typesInFile.isEmpty()) {
            String qualifiedTypeName = Joiner.on(".").skipNulls().join(typesInFile);
            String typeName = typesInFile.pop();
            types.put(typeName, currentPackage + "." + qualifiedTypeName);
        }
        super.endVisit(td);
    }

    @Override
    public boolean visit(MethodDeclaration node) {
        methodStack.push(node);
        MethodDecl methoDecl = getMethodDecl(node);
        declaredMethods.add(methoDecl);
        Javadoc javadoc = node.getJavadoc();
        ASTNode parent = node.getParent();
        if (javadoc != null && parent instanceof AbstractTypeDeclaration) {
            String typeName = ((AbstractTypeDeclaration) parent).getName().getFullyQualifiedName();
            String fqt = currentPackage + "." + removeSpecialSymbols(typeName);
            addMethodDoc(fqt, methoDecl, javadoc.toString());

        }
        return super.visit(node);
    }

    @Override
    public void endVisit(MethodDeclaration node) {
        if (!methodStack.isEmpty()) {
            methodStack.pop();
        }
        super.endVisit(node);
    }

    @Override
    public boolean visit(Assignment assignment) {
        return super.visit(assignment);
    }

    @SuppressWarnings("rawtypes")
    @Override
    public boolean visit(ClassInstanceCreation node) {
        List args = node.arguments();
        Map<String, Integer> scopeBindings = getNodeScopes().get(node);
        List<String> argTypes = translateArgsToTypes(args, scopeBindings);
        String type = getNameOfType(node.getType());
        if (!methodStack.empty()) {
            MethodDeclaration currentMethod = methodStack.peek();
            MethodDecl methodDecl = getMethodDecl(currentMethod);
            List<MethodInvokRef> invoks = methodInvoks.get(methodDecl);
            if (invoks == null) {
                invoks = new ArrayList<>();
                methodInvoks.put(methodDecl, invoks);
            }
            MethodInvokRef methodInvokRef = new MethodInvokRef(removeSpecialSymbols(node.getType().toString()),
                    type, "", args
                    .size(), node.getStartPosition(), argTypes, node.getLength(), true,
                    getReturnType(node));
            invoks.add(methodInvokRef);
        }
        return super.visit(node);
    }

    @SuppressWarnings("rawtypes")
    @Override
    public boolean visit(MethodInvocation node) {
        SimpleName methodName = node.getName();

        List args = node.arguments();
        Expression expression = node.getExpression();
        Map<String, Integer> scopeBindings = getNodeScopes().get(node);
        String target = getTarget(expression);

        String targetType = translateTargetToType(target, scopeBindings);
        // Add only if you could guess the type of target, else ignore.
        // TODO: In case of a method in super type, this will still infer it as in "this".
        if (!targetType.isEmpty()) {
            List<String> argTypes = translateArgsToTypes(args, scopeBindings);
            if (!methodStack.empty()) {
                MethodDeclaration currentMethod = methodStack.peek();
                MethodDecl methodDecl = getMethodDecl(currentMethod);
                List<MethodInvokRef> invoks = methodInvoks.get(methodDecl);
                if (invoks == null) {
                    invoks = new ArrayList<>();
                    methodInvoks.put(methodDecl, invoks);
                }
                MethodInvokRef methodInvokRef = new MethodInvokRef(methodName.toString(), targetType, target, args
                        .size(), node.getName().getStartPosition(), argTypes, methodName.getLength(), false,
                        getReturnType(node));
                invoks.add(methodInvokRef);
            }
        }
        return true;
    }

    @Override
    public boolean visit(org.eclipse.jdt.core.dom.EnumDeclaration ed) {
        String typeFullyQualifiedName = removeSpecialSymbols(ed.getName().getFullyQualifiedName());
        if (ed.superInterfaceTypes() != null && ed.superInterfaceTypes().size() > 0) {
            List<Object> interfaces = ed.superInterfaceTypes();
            typeToInterfacesFullyQualifiedName.put(typeFullyQualifiedName, interfaces);
        }
        typesInFile.push(ed.getName().getFullyQualifiedName());
        TypeDecl obj = new TypeDecl(typeFullyQualifiedName, ed.getName().getStartPosition());
        typeDeclarations.add(obj);

        addTypeDoc(ed, typeFullyQualifiedName);

        return true;
    }

    @Override
    public void endVisit(org.eclipse.jdt.core.dom.EnumDeclaration ed) {
        if (!typesInFile.isEmpty()) {
            String qualifiedTypeName = Joiner.on(".").skipNulls().join(typesInFile);
            String typeName = typesInFile.pop();
            types.put(typeName, currentPackage + "." + qualifiedTypeName);
        }
        super.endVisit(ed);
    }

    private void addTypeDoc(AbstractTypeDeclaration ed, String typeFullyQualifiedName) {
        String fullTypeName = currentPackage + "." + typeFullyQualifiedName;
        String docComment = "";
        if (ed.getJavadoc() != null) {
            docComment = ed.getJavadoc().toString();
        }
        typeJavadocs.put(fullTypeName, new TypeJavadoc(fullTypeName, docComment, new HashSet<MethodJavadoc>()));
    }

    private void addMethodDoc(String fqt, MethodDecl methodDecl, String methodDoc) {
        TypeJavadoc typeJavadoc = typeJavadocs.get(fqt);
        if (typeJavadoc != null) {
            List<String> args = Lists.newArrayList(methodDecl.getArgs().values());
            typeJavadoc.getMethodJavadocs().add(
                    new MethodJavadoc(methodDecl.getMethodName(),
                            args, methodDoc));
        }
    }

    /**
     * If the passed node is a method invocation or class creation then return the
     * return type of the method based on what is the returned value assigned to.
     *
     * @param node
     * @return return type
     */
    private String getReturnType(Expression node) {
        ASTNode parent = node.getParent();
        if (parent instanceof VariableDeclarationFragment) {
            ASTNode grandParent = parent.getParent();
            if (grandParent instanceof VariableDeclarationStatement) {
                Type typ = ((VariableDeclarationStatement) grandParent).getType();
                return removeSpecialSymbols(getFullyQualifiedNameFor(typ.toString()));
            }
        }
        return null;
    }

    private MethodDecl getMethodDecl(MethodDeclaration node) {
        String qualifiedTypeName = currentPackage + "."
                + Joiner.on(".").skipNulls().join(typesInFile);
        SimpleName nameNode = node.getName();
        String methodName = nameNode.toString();
        String returnType = "";
        if (node.getReturnType2() != null) {
            returnType = getNameOfType(node.getReturnType2());
        }

        Map<String, String> params = new HashMap<>();
        for (Object p : node.parameters()) {
            String typeName = OBJECT_TYPE;
            if (p instanceof SingleVariableDeclaration) {

                SingleVariableDeclaration svd = (SingleVariableDeclaration) p;
                String varName = svd.getName().toString();
                Type type = svd.getType();
                typeName = getNameOfType(type);

                params.put(varName, typeName);
            } else {
                System.err.println("Unxepected AST node type for param - " + p);
            }

        }
        return new MethodDecl(methodName, qualifiedTypeName, returnType, nameNode
                .getStartPosition(), params);
    }

    protected String translateTargetToType(String target,
                                           Map<String, Integer> scopeBindings) {
        String targetType = "";
        if (target != null && !target.isEmpty()) {
            if (target.equals("this")) {
                targetType = currentPackage + "." + Joiner.on(".").join(typesInFile);
            } else {
                final Integer variableId = scopeBindings.get(target);
                if (variableId == null
                        || !getVariableBinding().containsKey(variableId)) {

                    targetType = getFullyQualifiedNameFor(target);
                } else {
                    targetType = getVariableTypes().get(variableId);
                }
            }
        }
        return targetType;
    }

    private String getTarget(Expression expression) {
        String target = "";
        if (expression != null) {
            target = expression.toString().trim();
            if (target.isEmpty() || target.equals("this")) {
                return "this";
            }
            if (!target.isEmpty() && !isValidIdentifier(target)) {
                return "";
            }
        }
        return target;
    }

    private Boolean isValidIdentifier(String str) {
        if (str == null || str.isEmpty()) {
            return false;
        } else {
            char[] chars = str.toCharArray();
            if (!Character.isJavaIdentifierStart(chars[0])) {
                return false;
            }
            for (int i = 1; i < chars.length; i++) {
                if (!Character.isJavaIdentifierPart(chars[i])) {
                    return false;
                }
            }
        }
        return true;
    }

    @SuppressWarnings("rawtypes")
    protected List<String> translateArgsToTypes(List args,
                                                Map<String, Integer> scopeBindings) {
        List<String> argTypes = new ArrayList<String>();
        for (Object o : args) {
            String name = o.toString();
            Integer varId = scopeBindings.get(name);
            if (varId == null) {
                String staticTypeRef = getImportedNames().get(name);
                if (staticTypeRef != null) {
                    argTypes.add("<static>" + staticTypeRef);
                } else {
                    argTypes.add(OBJECT_TYPE);
                }
            } else {
                argTypes.add(getVariableTypes().get(varId));
            }
        }
        return argTypes;
    }

    public Map<String, List<String>> getInterfaces() {
        Map<String, List<String>> typeToInterfaces = new HashMap<>();
        for (String type : typeToInterfacesFullyQualifiedName.keySet()) {
            List<String> interfaces = new ArrayList<>();
            for (Object intrface : typeToInterfacesFullyQualifiedName.get(type)) {
                interfaces.add(removeSpecialSymbols(getFullyQualifiedNameFor(intrface.toString())));
            }
            typeToInterfaces.put(type, interfaces);
        }
        return typeToInterfaces;
    }

    public static class TypeDecl {
        private String className;
        private Integer loc;

        public TypeDecl(String className, Integer loc) {
            super();
            this.className = className;
            this.loc = loc;
        }

        public String getClassName() {
            return className;
        }

        public Integer getLoc() {
            return loc;
        }
    }

    public static class MethodDecl {
        private String methodName;
        private String enclosingType;
        private String returnType;
        private int location;
        private Map<String, String> args;

        public MethodDecl(String methodName, String enclosingType, String returnType, int location,
                          Map<String, String> args) {
            super();
            this.methodName = methodName;
            this.enclosingType = enclosingType;
            this.returnType = returnType;
            this.location = location;
            this.args = args;
        }

        public String getReturnType() {
            return returnType;
        }

        public String getMethodName() {
            return methodName;
        }

        public Integer getLocation() {
            return location;
        }

        public String getEnclosingType() {
            return enclosingType;
        }

        // Name -> Type
        public Map<String, String> getArgs() {
            return args;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            MethodDecl that = (MethodDecl) o;

            if (location != that.location) return false;
            if (!methodName.equals(that.methodName)) return false;
            if (!enclosingType.equals(that.enclosingType)) return false;
            if (!returnType.equals(that.returnType)) return false;
            return args.equals(that.args);

        }

        @Override
        public int hashCode() {
            int result = methodName.hashCode();
            result = 31 * result + enclosingType.hashCode();
            result = 31 * result + returnType.hashCode();
            result = 31 * result + location;
            result = 31 * result + args.hashCode();
            return result;
        }

        @Override
        public String toString() {
            return "MethodDecl{" +
                    "methodName='" + methodName + '\'' +
                    ", enclosingType='" + enclosingType + '\'' +
                    ", returnType='" + returnType + '\'' +
                    ", location=" + location +
                    ", args=" + args +
                    '}';
        }
    }

    public static class MethodInvokRef {
        private String methodName;
        private String targetType;
        private String target;
        private Integer argNum;
        private Integer location;
        private Integer length;
        private String returnType;
        private Boolean isConstructor;
        private List<String> argTypes;

        private MethodInvokRef(String methodName, String targetType, String target,
                               Integer argNum, Integer location, List<String> argTypes, Integer length,
                               Boolean isConstructor, String returnType) {
            super();
            this.methodName = methodName;
            this.targetType = targetType;
            this.argNum = argNum;
            this.location = location;
            this.argTypes = argTypes;
            this.target = target;
            this.length = length;
            this.returnType = returnType;
            this.isConstructor = isConstructor;

        }

        public Integer getLength() {
            return length;
        }

        public String getMethodName() {
            return methodName;
        }

        public void setMethodName(String methodName) {
            this.methodName = methodName;
        }

        public String getTargetType() {
            return targetType;
        }

        public void setTargetType(String targetType) {
            this.targetType = targetType;
        }


        public String getTarget() {
            return target;
        }

        public void setTarget(String target) {
            this.target = target;
        }

        public Integer getArgNum() {
            return argNum;
        }

        public void setArgNum(Integer argNum) {
            this.argNum = argNum;
        }

        public Integer getLocation() {
            return location;
        }

        public void setLocation(Integer location) {
            this.location = location;
        }

        public List<String> getArgTypes() {
            return argTypes;
        }

        public void setArgTypes(List<String> argTypes) {
            this.argTypes = argTypes;
        }

        public Boolean getConstructor() {
            return isConstructor;
        }

        public String getReturnType() {
            return returnType;
        }

        @Override
        public String toString() {
            return "MethodInvokRef{" +
                    "methodName='" + methodName + '\'' +
                    ", targetType='" + targetType + '\'' +
                    ", target='" + target + '\'' +
                    ", argNum=" + argNum +
                    ", location=" + location +
                    ", length=" + length +
                    ", returnType='" + returnType + '\'' +
                    ", isConstructor=" + isConstructor +
                    ", argTypes=" + argTypes +
                    '}';
        }
    }

    public static class JavaDoc {

        private String name;
        private String comment;

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public String getComment() {
            return comment;
        }

        public void setComment(String comment) {
            this.comment = comment;
        }

        public JavaDoc(String name, String comment) {
            this.name = name;
            this.comment = comment;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            JavaDoc javaDoc = (JavaDoc) o;

            return name.equals(javaDoc.name);

        }

        @Override
        public int hashCode() {
            return name.hashCode();
        }

        @Override
        public String toString() {
            return "JavaDoc{" +
                    "name='" + name + '\'' +
                    ", comment='" + comment + '\'' +
                    '}';
        }
    }

    public static class MethodJavadoc extends JavaDoc {
        private List<String> argTypes;

        public MethodJavadoc(String name, List<String> pArgTypes, String comment) {
            super(name, comment);
            this.argTypes = pArgTypes;
        }

        public List<String> getArgTypes() {
            return argTypes;
        }
    }

    public static class TypeJavadoc extends JavaDoc {

        private Set<MethodJavadoc> methodJavadocs;

        public TypeJavadoc(String name, String comment, Set<MethodJavadoc> methodJavadocs) {
            super(name, comment);
            this.methodJavadocs = methodJavadocs;
        }

        public Set<MethodJavadoc> getMethodJavadocs() {
            return methodJavadocs;
        }

        @Override
        public String toString() {
            return "TypeJavadoc{" +
                    "methodJavadocs=" + methodJavadocs +
                    "}, typeDoc=" + super.toString();
        }
    }

}
