import { parse } from "@babel/parser";
import * as esbuild from "esbuild";
import MagicString from "magic-string";
import resolveConfig from "tailwindcss/resolveConfig.js";
import fs from "fs";
import path from "path";

function findProjectRoot(startPath = process.cwd()) {
  try {
    let currentPath = startPath;
    let count = 0;
    while (currentPath !== path.parse(currentPath).root && count < 20) {
      if (fs.existsSync(path.join(currentPath, "package.json"))) {
        return currentPath;
      }
      currentPath = path.dirname(currentPath);
      count++;
    }
    return process.cwd();
  } catch (error) {
    console.error("Error locating project root:", error);
    return process.cwd();
  }
}

const threeFiberElements = [
  "object3D", "audioListener", "positionalAudio", "mesh", "batchedMesh",
  "instancedMesh", "scene", "sprite", "lOD", "skinnedMesh", "skeleton",
  "bone", "lineSegments", "lineLoop", "points", "group", "camera",
  "perspectiveCamera", "orthographicCamera", "cubeCamera", "arrayCamera",
  "instancedBufferGeometry", "bufferGeometry", "boxBufferGeometry",
  "circleBufferGeometry", "coneBufferGeometry", "cylinderBufferGeometry",
  "dodecahedronBufferGeometry", "extrudeBufferGeometry", "icosahedronBufferGeometry",
  "latheBufferGeometry", "octahedronBufferGeometry", "planeBufferGeometry",
  "polyhedronBufferGeometry", "ringBufferGeometry", "shapeBufferGeometry",
  "sphereBufferGeometry", "tetrahedronBufferGeometry", "torusBufferGeometry",
  "torusKnotBufferGeometry", "tubeBufferGeometry", "wireframeGeometry",
  "tetrahedronGeometry", "octahedronGeometry", "icosahedronGeometry",
  "dodecahedronGeometry", "polyhedronGeometry", "tubeGeometry",
  "torusKnotGeometry", "torusGeometry", "sphereGeometry", "ringGeometry",
  "planeGeometry", "latheGeometry", "shapeGeometry", "extrudeGeometry",
  "edgesGeometry", "coneGeometry", "cylinderGeometry", "circleGeometry",
  "boxGeometry", "capsuleGeometry", "material", "shadowMaterial",
  "spriteMaterial", "rawShaderMaterial", "shaderMaterial", "pointsMaterial",
  "meshPhysicalMaterial", "meshStandardMaterial", "meshPhongMaterial",
  "meshToonMaterial", "meshNormalMaterial", "meshLambertMaterial",
  "meshDepthMaterial", "meshDistanceMaterial", "meshBasicMaterial",
  "meshMatcapMaterial", "lineDashedMaterial", "lineBasicMaterial",
  "primitive", "light", "spotLightShadow", "spotLight", "pointLight",
  "rectAreaLight", "hemisphereLight", "directionalLightShadow",
  "directionalLight", "ambientLight", "lightShadow", "ambientLightProbe",
  "hemisphereLightProbe", "lightProbe", "spotLightHelper", "skeletonHelper",
  "pointLightHelper", "hemisphereLightHelper", "gridHelper", "polarGridHelper",
  "directionalLightHelper", "cameraHelper", "boxHelper", "box3Helper",
  "planeHelper", "arrowHelper", "axesHelper", "texture", "videoTexture",
  "dataTexture", "dataTexture3D", "compressedTexture", "cubeTexture",
  "canvasTexture", "depthTexture", "raycaster", "vector2", "vector3",
  "vector4", "euler", "matrix3", "matrix4", "quaternion", "bufferAttribute",
  "float16BufferAttribute", "float32BufferAttribute", "float64BufferAttribute",
  "int8BufferAttribute", "int16BufferAttribute", "int32BufferAttribute",
  "uint8BufferAttribute", "uint16BufferAttribute", "uint32BufferAttribute",
  "instancedBufferAttribute", "color", "fog", "fogExp2", "shape",
  "colorShiftMaterial"
];

const dreiElements = [
  "AsciiRenderer", "Billboard", "Clone", "ComputedAttribute", "Decal",
  "Edges", "Effects", "GradientTexture", "Image", "MarchingCubes",
  "Outlines", "PositionalAudio", "Sampler", "ScreenSizer", "ScreenSpace",
  "Splat", "Svg", "Text", "Text3D", "Trail", "CubeCamera",
  "OrthographicCamera", "PerspectiveCamera", "CameraControls", "FaceControls",
  "KeyboardControls", "MotionPathControls", "PresentationControls",
  "ScrollControls", "DragControls", "GizmoHelper", "Grid", "Helper",
  "PivotControls", "TransformControls", "CubeTexture", "Fbx", "Gltf",
  "Ktx2", "Loader", "Progress", "ScreenVideoTexture", "Texture",
  "TrailTexture", "VideoTexture", "WebcamVideoTexture", "CycleRaycast",
  "DetectGPU", "Example", "FaceLandmarker", "Fbo", "Html", "Select",
  "SpriteAnimator", "StatsGl", "Stats", "Trail", "Wireframe",
  "CurveModifier", "AdaptiveDpr", "AdaptiveEvents", "BakeShadows", "Bvh",
  "Detailed", "Instances", "Merged", "meshBounds", "PerformanceMonitor",
  "Points", "Preload", "Segments", "Fisheye", "Hud", "Mask",
  "MeshPortalMaterial", "RenderCubeTexture", "RenderTexture", "View",
  "MeshDiscardMaterial", "MeshDistortMaterial", "MeshReflectorMaterial",
  "MeshRefractionMaterial", "MeshTransmissionMaterial", "MeshWobbleMaterial",
  "PointMaterial", "shaderMaterial", "SoftShadows", "CatmullRomLine",
  "CubicBezierLine", "Facemesh", "Line", "Mesh", "QuadraticBezierLine",
  "RoundedBox", "ScreenQuad", "AccumulativeShadows", "Backdrop", "BBAnchor",
  "Bounds", "CameraShake", "Caustics", "Center", "Cloud", "ContactShadows",
  "Environment", "Float", "Lightformer", "MatcapTexture", "NormalTexture",
  "RandomizedLight", "Resize", "ShadowAlpha", "Shadow", "Sky", "Sparkles",
  "SpotLightShadow", "SpotLight", "Stage", "Stars", "OrbitControls"
];

function shouldTagComponent(elementName) {
  return !threeFiberElements.includes(elementName) && !dreiElements.includes(elementName);
}

const validFileExtensions = new Set([".jsx", ".tsx"]);
const projectRootDir = findProjectRoot();
const tailwindConfigInput = path.resolve(projectRootDir, "./tailwind.config.ts");
const tailwindJsonOutput = path.resolve(projectRootDir, "./src/tailwind.config.custom.json");
const tailwindTempFile = path.resolve(projectRootDir, "./.custom.tailwind.config.js");
const isDevSandbox = process.env.CUSTOM_DEV_SERVER === "true";

async function generateTailwindConfig() {
  try {
    await esbuild.build({
      entryPoints: [tailwindConfigInput],
      outfile: tailwindTempFile,
      bundle: true,
      format: "esm",
      banner: {
        js: 'import { createRequire } from "module"; const require = createRequire(import.meta.url);'
      }
    });

    const configModule = await import(`${tailwindTempFile}?update=${Date.now()}`);
    if (!configModule?.default) {
      console.error("Invalid Tailwind configuration structure");
      throw new Error("Invalid Tailwind configuration structure");
    }

    const resolvedConfig = resolveConfig(configModule.default);
    await fs.writeFile(tailwindJsonOutput, JSON.stringify(resolvedConfig, null, 2));
    await fs.unlink(tailwindTempFile).catch(() => {});
  } catch (error) {
    console.error("Failed to generate Tailwind config:", error);
    throw error;
  }
}

function componentTagger() {
  const cwd = process.cwd();
  const stats = {
    totalFiles: 0,
    processedFiles: 0,
    totalComponents: 0
  };

  return {
    name: "vite-plugin-component-tagger",
    enforce: "pre",
    async transform(code, id) {
      if (!validFileExtensions.has(path.extname(id)) || id.includes("node_modules")) {
        return null;
      }

      stats.totalFiles++;
      const relativeFilePath = path.relative(cwd, id);

      try {
        const ast = parse(code, {
          sourceType: "module",
          plugins: ["jsx", "typescript"]
        });

        const magicString = new MagicString(code);
        let modifiedComponents = 0;
        let currentJSXElement = null;

        const { walk } = await import("estree-walker");
        walk(ast, {
          enter(node) {
            if (node.type === "JSXElement") {
              currentJSXElement = node;
            }
            if (node.type === "JSXOpeningElement") {
              let componentName;
              if (node.name.type === "JSXIdentifier") {
                componentName = node.name.name;
              } else if (node.name.type === "JSXMemberExpression") {
                componentName = `${node.name.object.name}.${node.name.property.name}`;
              } else {
                return;
              }

              const attributes = node.attributes.reduce((acc, attr) => {
                if (attr.type === "JSXAttribute") {
                  if (attr.value?.type === "StringLiteral") {
                    acc[attr.name.name] = attr.value.value;
                  } else if (attr.value?.type === "JSXExpressionContainer" && 
                           attr.value.expression.type === "StringLiteral") {
                    acc[attr.name.name] = attr.value.expression.value;
                  }
                }
                return acc;
              }, {});

              let textContent = "";
              if (currentJSXElement?.children) {
                textContent = currentJSXElement.children
                  .map(child => {
                    if (child.type === "JSXText") {
                      return child.value.trim();
                    } else if (child.type === "JSXExpressionContainer" && 
                             child.expression.type === "StringLiteral") {
                      return child.expression.value;
                    }
                    return "";
                  })
                  .filter(Boolean)
                  .join(" ")
                  .trim();
              }

              const contentData = {};
              if (textContent) contentData.text = textContent;
              if (attributes.className) contentData.className = attributes.className;
              if (attributes.placeholder) contentData.placeholder = attributes.placeholder;

              const line = node.loc?.start?.line ?? 0;
              const col = node.loc?.start?.column ?? 0;
              const componentId = `${relativeFilePath}:${line}:${col}`;
              const fileName = path.basename(id);

              if (shouldTagComponent(componentName)) {
                const dataAttributes = ` data-custom-id="${componentId}" data-custom-name="${componentName}"` +
                  ` data-comp-path="${relativeFilePath}" data-comp-line="${line}"` +
                  ` data-comp-file="${fileName}" data-comp-name="${componentName}"` +
                  ` data-comp-content="${encodeURIComponent(JSON.stringify(contentData))}"`;

                magicString.appendLeft(node.name.end ?? 0, dataAttributes);
                modifiedComponents++;
              }
            }
          }
        });

        stats.processedFiles++;
        stats.totalComponents += modifiedComponents;

        return {
          code: magicString.toString(),
          map: magicString.generateMap({ hires: true })
        };
      } catch (error) {
        console.error(`Error processing ${relativeFilePath}:`, error);
        stats.processedFiles++;
        return null;
      }
    },

    async buildStart() {
      if (!isDevSandbox) return;
      try {
        await generateTailwindConfig();
      } catch (error) {
        console.error("Failed to generate custom Tailwind config:", error);
      }
    },

    configureServer(server) {
      if (!isDevSandbox) return;
      try {
        server.watcher.add(tailwindConfigInput);
        server.watcher.on("change", async (changedPath) => {
          if (path.normalize(changedPath) === path.normalize(tailwindConfigInput)) {
            await generateTailwindConfig();
          }
        });
      } catch (error) {
        console.error("Error setting up watcher:", error);
      }
    }
  };
}

export { componentTagger };