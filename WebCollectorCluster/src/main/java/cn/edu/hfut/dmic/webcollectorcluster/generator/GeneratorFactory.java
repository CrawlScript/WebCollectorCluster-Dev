/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package cn.edu.hfut.dmic.webcollectorcluster.generator;

/**
 *
 * @author hu
 */
public interface GeneratorFactory {
    public Generator createGenerator(Generator generator);
}
